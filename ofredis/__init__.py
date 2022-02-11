import base64
import hashlib
import logging
import re
import uuid

import dateutil.parser
import kopf
import pykube
import pyredis
import pyredis.exceptions

logging.getLogger("urllib3").setLevel(logging.WARNING)

sha256_regex = re.compile('^[A-Fa-f0-9]{64}$')
redis_num_unit_convert = re.compile('^[0-9]*(k|kb|m|mb|g|fb)$')


redis_config_params_filter = [
    'appendfilename',
    'dbfilename',
    'masterauth',
    'masteruser',
    'protected-mode',
    'requirepass',
    'replica-announce-ip',
    'replica-announce-port',
]


@kopf.on.startup()
async def configure(settings: kopf.OperatorSettings, **kwargs):
    settings.execution.max_workers = 10000


@kopf.daemon('redis-replications')
def redis_monitor_daemon(stopped, logger, spec, name, namespace, **__):
    redis_repl = RedisReplication(
        logger=logger,
        name=name,
        namespace=namespace,
        spec=spec,
        stopped=stopped
    )
    redis_repl.run()


class PyKubeRedisReplication(pykube.objects.NamespacedAPIObject):
    version = 'kopf.dev/v1'
    endpoint = 'redis-replications'
    kind = 'RedisReplication'


class RedisAcl:
    def __init__(
            self,
            logger,
            username,
            user_enable,
            commands=None,
            key_patterns=None,
            passwords=None,
            pubsub_patterns=None
    ):
        self._username = username
        self._user_enable = None
        self._commands = set()
        self._key_patterns = set()
        self._passwords = set()
        self._pubsub_patterns = set()

        self.log = logger
        self.commands = commands
        self.key_patterns = key_patterns
        self.passwords = passwords
        self.pubsub_patterns = pubsub_patterns
        self.user_enable = user_enable

    @property
    def commands(self):
        if '+@all' in self._commands:
            self._commands = {'+@all'}
        elif not self._commands:
            self._commands = {'-@all'}
        elif '-@all' not in self._commands:
            self._commands.add('-@all')
        return self._commands

    @commands.setter
    def commands(self, value):
        if value is None:
            self._commands = set()
        elif isinstance(value, list):
            self._commands = set(value)
        else:
            raise ValueError("commands is expected to be a list")

    @property
    def key_patterns(self):
        if '~*' in self._key_patterns:
            self._key_patterns = {'~*'}
        return self._key_patterns

    @key_patterns.setter
    def key_patterns(self, value):
        if value is None:
            self._key_patterns = set()
        elif isinstance(value, list):
            self._key_patterns = set(value)
        else:
            raise ValueError("keyPatterns is expected to be a list")

    @property
    def passwords(self):
        return self._passwords

    @passwords.setter
    def passwords(self, value):
        if value is None:
            self._passwords = set()
        elif isinstance(value, list):
            self._passwords = set(value)
        else:
            raise ValueError("passwords is expected to be a list")

    @property
    def pubsub_patterns(self):
        if '&*' in self._pubsub_patterns:
            self._pubsub_patterns = {'&*'}
        return self._pubsub_patterns

    @pubsub_patterns.setter
    def pubsub_patterns(self, value):
        if value is None:
            self._pubsub_patterns = set()
        elif isinstance(value, list):
            self._pubsub_patterns = set(value)
        else:
            raise ValueError("pubsubPatterns is expected to be a list")

    @property
    def username(self):
        return self._username

    @property
    def user_enable(self):
        return self._user_enable

    @user_enable.setter
    def user_enable(self, enable):
        if enable not in ['on', 'off']:
            raise ValueError("user_enable needs to be either 'on' or 'off'")
        self._user_enable = enable


class RedisReplication:
    def __init__(self, stopped, spec, logger, name, namespace):
        self._api = None
        self._log = logger
        self._name = name
        self._namespace = namespace
        self._redis = dict()
        self._redis_acl_operator = None
        self._redis_acl_repl = None
        self._redis_operator_secrets = None
        self._spec = spec
        self._stopped = stopped

    @property
    def api(self):
        if not self._api:
            self._api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        return self._api

    @property
    def log(self):
        return self._log

    @property
    def name(self):
        return self._name

    @property
    def namespace(self):
        return self._namespace

    @property
    def operator_status(self):
        return PyKubeRedisReplication.objects(
            self.api,
            namespace=self.namespace
        ).get_by_name(self.name)

    @property
    def pods(self):
        pods = pykube.Pod.objects(
            self.api
        ).filter(
            namespace=self.namespace,
            selector={'RedisReplication': self.name}
        )
        return self._pod_remove_deleted(pods=pods)

    @property
    def pod_primary(self):
        pods = self.pod_primaries
        if len(pods) == 0:
            return None
        if len(pods) == 1:
            return pods.pop()
        self.log.error("to many primary nodes, found {0} nodes".format(len(pods)))
        raise RedisReplicationErrorToManyPrimaries

    @property
    def pod_primary_name(self):
        pod = self.pod_primary
        if pod:
            return pod.name

    @property
    def pod_primaries(self):
        pods = pykube.Pod.objects(
            self.api
        ).filter(
            namespace=self.namespace,
            selector={
                'RedisReplication': self.name,
                'RedisReplicationRole': 'Primary',
            }
        )
        return self._pod_remove_deleted(pods=pods)

    @property
    def pod_secondaries(self):
        pods = pykube.Pod.objects(
            self.api
        ).filter(
            namespace=self.namespace,
            selector={
                'RedisReplication': self.name,
                'RedisReplicationRole': 'Secondary',
            }
        )
        return self._pod_remove_deleted(pods=pods)

    @property
    def redis(self):
        return self._redis

    @property
    def redis_acl_operator(self):
        if not self._redis_acl_operator:
            secrets = self.redis_operator_secrets
            self._redis_acl_operator = RedisAcl(
                logger=self.log,
                username=secrets['OperatorUsername'],
                user_enable='on',
                passwords=[
                    "#{0}".format(hashlib.sha256(secrets['OperatorPassword'].encode()).hexdigest())
                ],
                commands=['+acl', '+config', '+info', '+ping', '+replicaof']
            )
        return self._redis_acl_operator

    @property
    def redis_acl_repl(self):
        if not self._redis_acl_repl:
            secrets = self.redis_operator_secrets
            self._redis_acl_repl = RedisAcl(
                logger=self.log,
                username=secrets['ReplUsername'],
                user_enable='on',
                passwords=[
                    "#{0}".format(hashlib.sha256(secrets['ReplPassword'].encode()).hexdigest())
                ],
                commands=['+psync', '+replconf', '+ping']
            )
        return self._redis_acl_repl

    def _redis_operator_secrets_create(self):
        secrets = {
            "OperatorUsername": str(uuid.uuid4()),
            "OperatorPassword": str(uuid.uuid4()),
            "ReplUsername": str(uuid.uuid4()),
            "ReplPassword": str(uuid.uuid4()),
        }
        secret_data = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "{0}-operator".format(self.name)},
            "type": "Opaque",
            "data": {
                "OperatorUsername": base64.b64encode(secrets["OperatorUsername"].encode()).decode(),
                "OperatorPassword": base64.b64encode(secrets["OperatorPassword"].encode()).decode(),
                "ReplUsername": base64.b64encode(secrets["ReplUsername"].encode()).decode(),
                "ReplPassword": base64.b64encode(secrets["ReplPassword"].encode()).decode(),
            }
        }
        kopf.adopt(secret_data)
        kopf.label(secret_data, {'RedisReplication': self.name})
        while True:
            try:
                self.log.info('Creating Secret {0}-operator'.format(self.name))
                print(pykube.Secret(self.api, secret_data).create())
                self.log.info('Creating Secret {0}-operator, done'.format(self.name))
                return secrets
            except pykube.exceptions.KubernetesError as err:
                self.log.error('Creating Secret {0}-operator failed, {1}'.format(self.name, err))
                self.log.error("retrying in 10 seconds")
                self.stopped.wait(10)

    def _redis_operator_secrets_get(self):
        try:
            secrets = pykube.Secret.objects(
                self.api
            ).filter(
                namespace=self.namespace
            ).get(
                name="{0}-operator".format(self.name)
            )
            secrets = secrets.obj['data']
            secrets = {
                "OperatorUsername": base64.b64decode(secrets["OperatorUsername"].encode()).decode(),
                "OperatorPassword": base64.b64decode(secrets["OperatorPassword"].encode()).decode(),
                "ReplUsername": base64.b64decode(secrets["ReplUsername"].encode()).decode(),
                "ReplPassword": base64.b64decode(secrets["ReplPassword"].encode()).decode(),
            }
        except pykube.exceptions.ObjectDoesNotExist:
            secrets = self._redis_operator_secrets_create()
        return secrets

    @property
    def redis_operator_secrets(self):
        if not self._redis_operator_secrets:
            self._redis_operator_secrets = self._redis_operator_secrets_get()
        return self._redis_operator_secrets

    @property
    def spec(self):
        return self._spec

    @property
    def spec_acls(self):
        acls = dict()
        for username, acl_spec in self.spec['acls'].items():
            if username in ['RedisOperator', 'RedisRepl']:
                self.log.warning('removing reserved username {0} from acl spec'.format(username))
                continue
            user_enable = acl_spec.get('userEnable', False)
            if user_enable:
                user_enable = 'on'
            else:
                user_enable = 'off'
            commands = acl_spec.get('commands', None)
            key_patterns = acl_spec.get('keyPatterns', None)
            pubsub_patterns = acl_spec.get('pubsubPatterns', None)
            passwords = list()
            for secret_spec in acl_spec.get('passwords', dict()):
                password = self.password(
                    secret_name=secret_spec['secretName'],
                    secret_data_key=secret_spec['secretDataKey']
                )
                if password == 'nopass':
                    passwords = [password]
                    break
                if sha256_regex.match(password):
                    passwords.append("#{0}".format(password))
                else:
                    passwords.append("#{0}".format(
                        hashlib.sha256(password.encode()).hexdigest())
                    )
            acl = RedisAcl(
                logger=self.log,
                username=username,
                user_enable=user_enable,
                commands=commands,
                key_patterns=key_patterns,
                pubsub_patterns=pubsub_patterns,
                passwords=passwords
            )
            acls[username] = acl
        return acls

    @property
    def stopped(self):
        return self._stopped

    @staticmethod
    def _pod_remove_deleted(pods):
        result = list()
        for pod in pods:
            if 'deletionTimestamp' not in pod.metadata:
                result.append(pod)
        return result

    def password(self, secret_name, secret_data_key):
        try:
            secret = pykube.Secret.objects(
                self.api
            ).filter(
                namespace=self.namespace
            ).get(
                name=secret_name
            )
        except pykube.exceptions.ObjectDoesNotExist:
            self.log.error('Secret {0} not found'.format(
                secret_name
            ))
            raise RedisReplicationSecretMissing
        try:
            return base64.b64decode(secret.obj['data'][secret_data_key].encode()).decode()
        except KeyError:
            self.log.error('Secret {0} has no DataKey {1}'.format(
                secret_name, secret_data_key
            ))
            raise RedisReplicationSecretMissing

    def pod_create(self):
        pod_data = {
            "apiVersion": "v1",
            "kind": "Pod",
            "spec": {
                "restartPolicy": 'Never',
                "containers": [
                    {
                        "name": self.name,
                        "image": self.spec['redis']['image'],
                        "ports": [
                            {
                                "containerPort": 6379
                            }
                        ]
                    }
                ]
            }
        }

        # Make it our child: assign the namespace, name, labels, owner references, etc.
        kopf.adopt(pod_data)
        kopf.label(pod_data, {'RedisReplication': self.name})

        # Actually create an object by requesting the Kubernetes API.
        pykube.Pod(self.api, pod_data).create()

    def pod_delete(self, pod):
        self.log.info("{0} deleting pod".format(pod.name))
        pod.delete()
        self.redis.pop(pod.name, None)
        self.log.info("{0} deleting pod, done".format(pod.name))

    def pod_delete_candidates(self):
        candidates = []
        self.log.info("trying to find unconfigured pod")
        for pod in self.pods:
            if 'RedisReplicationRole' not in pod.labels:
                candidates.append(pod)

        if candidates:
            self.log.info("found unconfigured pods")
            return candidates
        self.log.info("trying to find unconfigured pod, failed, passing secondaries")
        return self.pod_secondaries

    def pod_ensure_count(self):
        num_pods = len(self.pods)
        while num_pods < self.spec['replicas']:
            self.log.info("we have {0} of {1} replicas".format(
                num_pods,
                self.spec['replicas']
            ))
            self.pod_create()
            self.update_object_status()
            num_pods += 1
        while num_pods > self.spec['replicas']:
            self.log.info("we have {0} of {1} replicas".format(
                num_pods,
                self.spec['replicas']
            ))
            candidate = self.pod_delete_candidates().pop()
            print(candidate)
            self.pod_delete(pod=candidate)
            self.update_object_status()
            num_pods -= 1

    def pod_get_by_name(self, pod_name):
        return pykube.Pod.objects(
            self.api
        ).filter(
            namespace=self.namespace,
            selector={'RedisReplication': self.name}
        ).get(name=pod_name)

    def pod_is_ready(self, pod):
        self.log.info("{0} checking if pod is running".format(pod.name))
        self.log.info("{0} pod in {1} phase".format(
            pod.name, pod.obj['status']['phase']
        ))
        if pod.obj['status']['phase'] == 'Running':
            self.log.info("{0} pod is ready".format(pod.name))
            return pod
        elif pod.obj['status']['phase'] == 'Pending':
            raise RedisReplicationPodNotReady('{0} pod not ready'.format(pod.name))
        elif pod.obj['status']['phase'] == 'Failed':
            raise RedisReplicationPodError("{0} pod in error state".format(pod.name))
        elif pod.obj['status']['phase'] == 'Succeeded':
            raise RedisReplicationPodError("{0} pod in succeeded state".format(pod.name))
        elif pod.obj['status']['phase'] == 'Unknown':
            raise RedisReplicationPodError("{0} pod in unknown state".format(pod.name))
        else:
            raise RedisReplicationPodError("{0} pod in and unsupported state".format(pod.name))

    def pod_set_label(self, pod, label_name, label_value, retry=3):
        self.log.info("{0} setting label {1} with value {2} on pod".format(
            pod.name, label_name, label_value
        ))
        while retry > 0:
            try:
                pod.labels[label_name] = str(label_value)
                pod.update()
                return
            except pykube.exceptions.HTTPError as err:
                self.log.warning("{0} could not update pod: {1}, retrying".format(pod.name, err))
                self.stopped.wait(1)
                retry -= 1
                pod = self.pod_get_by_name(pod_name=pod.name)

        raise RedisReplicationRedisConnError("{0} could not update pod, no more retires left".format(pod.name))

    def redis_client_connect(self, pod):
        self.log.info("{0} connecting to redis".format(pod.name))
        self.pod_is_ready(pod=pod)
        ip_addr = pod.obj['status']['podIP']
        try:
            if 'RedisReplicationOperatorACLPresent' not in pod.labels:
                self.redis_create_operator_acls(pod=pod, ip_addr=ip_addr)

            self.log.info("{0} trying to login with redis-operator credentials".format(pod.name))
            client = pyredis.Client(
                host=ip_addr,
                username=self.redis_operator_secrets['OperatorUsername'],
                password=self.redis_operator_secrets['OperatorPassword']
            )
            client.ping()
            self.redis[pod.name] = client
            self.log.info("{0} trying to login with redis-operator credentials, success".format(pod.name))
        except pyredis.exceptions.ReplyError as err:
            raise RedisReplicationRedisConnError(err)
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ) as err:
            self.log.debug(err)
            self.log.error("{0} connection to pod went away".format(pod.name))
            raise RedisReplicationRedisConnError("{0} pod went away".format(pod.name))
        self.log.info("{0} connecting to redis, done".format(pod.name))

    def redis_client_get(self, pod):
        if pod.name not in self.redis:
            self.redis_client_connect(pod=pod)
        return self.redis[pod.name]

    def redis_create_operator_acls(self, pod, ip_addr):
        self.log.info("{0} trying to login without username/password".format(pod.name))
        client = pyredis.Client(host=ip_addr)
        client.ping()
        self.log.info("{0} trying to login without username/password, success".format(pod.name))
        self.log.info("{0} creating operator acl".format(pod.name))
        self.redis_acl_enforce(
            pod=pod,
            username=self.redis_acl_operator.username,
            redis_acl=None,
            spec_acl=self.redis_acl_operator,
            client=client
        )
        self.log.info("{0} creating operator acl, done".format(pod.name))
        self.log.info("{0} creating replication acl".format(pod.name))
        self.redis_acl_enforce(
            pod=pod,
            username=self.redis_acl_repl.username,
            redis_acl=None,
            spec_acl=self.redis_acl_repl,
            client=client
        )
        self.log.info("{0} creating replication acl, done".format(pod.name))
        self.log.info("{0} setting RedisReplicationOperatorACLPresent label".format(pod.name))
        self.pod_set_label(
            pod=pod,
            label_name='RedisReplicationOperatorACLPresent',
            label_value=True
        )
        self.log.info("{0} setting RedisReplicationOperatorACLPresent label, done".format(pod.name))

    def redis_config_enforce(self, pod):
        client = self.redis_client_get(pod)
        for option, target_value in self.spec.get('config', {}).items():
            if option in redis_config_params_filter:
                continue
            try:
                current_value = client.execute('CONFIG', 'GET', option)
            except (
                    pyredis.exceptions.PyRedisConnClosed,
                    pyredis.exceptions.PyRedisConnError,
                    pyredis.exceptions.PyRedisConnReadTimeout
            ):
                self.log.error("{0} connection to pod went away".format(pod.name))
                raise RedisReplicationRedisConnError("pod went away")
            target_value = self.redis_config_encode(value=target_value)
            if not current_value:
                self.log.warning("{0} option not found in redis, ignoring".format(option))
                continue
            if current_value[1] != target_value:
                self.log.info("{0} option {1} current value {2} not matching {3}, adjusting".format(
                    pod.name, option, current_value, target_value
                ))
                client.execute('CONFIG', 'SET', option, target_value)

    @staticmethod
    def redis_config_encode(value):
        if redis_num_unit_convert.match(value):
            if value.endswith('k'):
                value = str(int(value[:-1])*1000)
            elif value.endswith('kb'):
                value = str(int(value[:-2])*1024)
            elif value.endswith('m'):
                value = str(int(value[:-1])*1000*1000)
            elif value.endswith('mb'):
                value = str(int(value[:-2])*1024*1024)
            elif value.endswith('g'):
                value = str(int(value[:-1])*1000*1000*1000)
            elif value.endswith('gb'):
                value = str(int(value[:-2])*1024*1024*1024)
        return value.encode()

    @staticmethod
    def _redis_acl_add_password(command, passwords):
        if 'nopass' in passwords:
            command.append('nopass')
        else:
            command.append('resetpass')
            command.extend(list(passwords))

    def redis_acl_enforce(self, pod, username, redis_acl, spec_acl, client=None):
        if not client:
            client = self.redis_client_get(pod=pod)
        command = ['ACL', 'SETUSER', username]
        changes = False

        if redis_acl:
            if not redis_acl.user_enable == spec_acl.user_enable:
                changes = True
                command.append(spec_acl.user_enable)
            if redis_acl.commands != spec_acl.commands:
                changes = True
                _commands = sorted(spec_acl.commands)
                _commands.reverse()
                command.extend(_commands)
            if redis_acl.key_patterns != spec_acl.key_patterns:
                changes = True
                command.append('resetkeys')
                command.extend(list(spec_acl.key_patterns))
            if redis_acl.passwords != spec_acl.passwords:
                changes = True
                self._redis_acl_add_password(command=command, passwords=spec_acl.passwords)
            if redis_acl.pubsub_patterns != spec_acl.pubsub_patterns:
                changes = True
                command.append('resetchannels')
                command.extend(list(spec_acl.pubsub_patterns))
        else:
            changes = True
            command.append(spec_acl.user_enable)
            _commands = sorted(spec_acl.commands)
            _commands.reverse()
            command.extend(_commands)
            command.extend(list(spec_acl.key_patterns))
            self._redis_acl_add_password(command=command, passwords=spec_acl.passwords)
            command.append('resetchannels')
            command.extend(list(spec_acl.pubsub_patterns))

        if changes:
            self.log.info("{0} ACL changes acl: {1} detected, executing: {2}".format(
                pod.name, username, command
            ))
            try:
                client.execute(*command)
            except (
                    pyredis.exceptions.PyRedisConnClosed,
                    pyredis.exceptions.PyRedisConnError,
                    pyredis.exceptions.PyRedisConnReadTimeout
            ) as err:
                self.log.error("{0} connection to pod went away: {1}".format(pod.name, err))
                raise RedisReplicationRedisConnError("{0} pod went away".format(pod.name))
        return changes

    def redis_acls_enforce(self, pod):
        redis_acls = self.redis_acl_list(pod=pod)
        try:
            spec_acls = self.spec_acls
        except RedisReplicationSecretMissing:
            return

        for username, spec_acl in spec_acls.items():
            redis_acl = redis_acls.get(username, None)
            self.redis_acl_enforce(
                pod=pod,
                username=username,
                redis_acl=redis_acl,
                spec_acl=spec_acl
            )

        for username, redis_acl in redis_acls.items():
            if username in [
                self.redis_acl_repl.username,
                self.redis_acl_operator.username
            ]:
                continue
            if username not in spec_acls:
                client = self.redis_client_get(pod=pod)
                if username == 'default':
                    default_user = RedisAcl(
                        logger=self.log,
                        username='default',
                        user_enable='off'
                    )
                    self.redis_acl_enforce(
                        pod=pod,
                        username=username,
                        redis_acl=redis_acl,
                        spec_acl=default_user
                    )
                else:
                    self.log.info("{0} deleting user {1}".format(pod.name, username))
                    client.execute('ACL', 'DELUSER', username)
                    self.log.info("{0} deleting user {1}, done".format(pod.name, username))

    def redis_acl_list(self, pod):
        client = self.redis_client_get(pod=pod)
        try:
            _acls = client.execute('ACL', 'LIST')
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ):
            self.log.error("{0} connection to pod went away".format(pod.name))
            raise RedisReplicationRedisConnError("{0} pod went away".format(pod.name))
        acls = dict()
        for acl in _acls:
            acl = acl.decode()
            self.redis_acl_parse(acl=acl, acls=acls)

        return acls

    def redis_acl_parse(self, acl, acls):
        acl = acl.split()
        acl.reverse()
        acl.pop()
        username = acl.pop()
        user_enable = acl.pop()

        _acl = RedisAcl(
            logger=self.log,
            username=username,
            user_enable=user_enable
        )
        acls[username] = _acl
        for item in acl:
            if item.startswith('+') or item.startswith('-'):
                _acl.commands.add(item)
            elif item.startswith('~'):
                _acl.key_patterns.add(item)
            elif item.startswith('&'):
                _acl.pubsub_patterns.add(item)
            elif item.startswith('#'):
                _acl.passwords.add(item)
            elif item == 'nopass':
                _acl.passwords.add(item)

    def redis_cleanup(self):
        redis_delete = list()
        for redis in self.redis:
            present = False
            for pod in self.pods:
                if pod.name == redis:
                    present = True
            if not present:
                self.log.info("{0} removed from kubernetes, dropping connection".format(redis))
                redis_delete.append(redis)
        for redis in redis_delete:
            self.redis.pop(redis)

    def redis_primary_enforce(self):
        primary = self.pod_primary
        if not primary:
            self.log.info("no primary present")
            pod = self.redis_replication_primary_get_candidate()
            try:
                self.redis_replication_primary_promote(pod=pod)
            except RedisReplicationPodNotReady:
                pass
            except RedisReplicationRedisConnError as err:
                self.redis.pop(pod.name, None)
                self.log.error("{0} lost connection to redis on pod: {1}".format(
                    pod, err
                ))
            except RedisReplicationPodError:
                self.pod_delete(pod=pod)

        else:
            if primary.name != self.operator_status.obj['status']['master']:
                self.operator_status.patch({"status": {"master": primary.name}}, subresource='status')

    def redis_replication_primary_get_candidate(self):
        primary = None
        for pod in self.pods:
            if not primary:
                primary = pod
            elif dateutil.parser.parse(pod.obj['status']['startTime']) < dateutil.parser.parse(
                    primary.obj['status']['startTime']):
                primary = pod
        return primary

    def redis_replication_primary_promote(self, pod):
        self.log.info('{0} promoting pod to primary'.format(pod))
        client = self.redis_client_get(pod=pod)
        try:
            client.execute('replicaof', 'NO', 'ONE')
            client.execute('config', 'SET', 'masterauth', '')
            client.execute('config', 'SET', 'masteruser', '')
            self.operator_status.patch({"status": {"master": pod.name}}, subresource='status')
            self.pod_set_label(
                pod=pod,
                label_name='RedisReplicationRole',
                label_value='Primary'
            )
            self.log.info('{0} promoting pod to primary, success'.format(pod))
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ) as err:
            self.log.error("{0} connection to pod went away".format(pod.name))
            raise RedisReplicationRedisConnError(err)

    def redis_replication_secondary_enforce(self, pod):
        primary = self.pod_primary
        primary_name = self.pod_primary_name
        if not primary:
            self.log.warning("{0} no primary detected, skipping secondary enforcement".format(pod.name))
            return
        try:
            primary_ip = primary.obj['status']['podIP']
        except KeyError:
            self.log.warning("{0} could not get PodIP from primary pod {1}".format(pod.name, primary_name))
            return
        secrets = self.redis_operator_secrets
        if pod.name == primary_name:
            return
        try:
            client = self.redis_client_get(pod=pod)
            replof = (client.execute('config', 'get', 'replicaof'))[1]
            replof = replof.decode()
            if replof.startswith(primary_ip):
                return
            self.log.info("{0} making pod replica of pod {1}".format(
                pod.name, primary_name
            ))
            client.execute('config', 'SET', 'masterauth', secrets['ReplPassword'])
            client.execute('config', 'SET', 'masteruser', secrets['ReplUsername'])
            client.execute('replicaof', primary_ip, '6379')
            self.pod_set_label(
                pod=pod,
                label_name='RedisReplicationRole',
                label_value='Secondary'
            )
            self.log.info("{0} making pod replica of pod {1}, success".format(
                pod.name, primary_name
            ))
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ):
            self.log.error("{0} connection to pod went away".format(pod.name))
            raise RedisReplicationRedisConnError

    def update_object_status(self):
        self.operator_status.patch(
            {
                "status": {
                    "master": str(self.pod_primary_name),
                    "replicas": len(self.pods)
                }
            },
            subresource='status'
        )

    def run(self):
        self.log.info("starting daemon name: {0} namespace: {1}".format(
            self.name, self.namespace
        ))
        self.update_object_status()
        while not self.stopped:
            self.pod_ensure_count()
            self.redis_primary_enforce()
            self.redis_cleanup()
            for pod in self.pods:
                try:
                    self.redis_acls_enforce(pod=pod)
                    self.redis_config_enforce(pod=pod)
                    self.redis_replication_secondary_enforce(pod=pod)
                except RedisReplicationPodError:
                    self.pod_delete(pod=pod)
                except RedisReplicationPodNotReady:
                    pass
                except RedisReplicationRedisConnError as err:
                    self.redis.pop(pod.name, None)
                    self.log.error("{0} lost connection to redis on pod: {1}".format(
                        pod, err
                    ))
            self.stopped.wait(1)
        self.log.info("stopping daemon")


class RedisReplicationError(Exception):
    pass


class RedisReplicationPodError(RedisReplicationError):
    pass


class RedisReplicationRedisConnError(RedisReplicationError):
    pass


class RedisReplicationErrorToManyPrimaries(RedisReplicationError):
    pass


class RedisReplicationPodNotReady(RedisReplicationError):
    pass


class RedisReplicationSecretMissing(RedisReplicationError):
    pass

