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


class KEX(pykube.objects.NamespacedAPIObject):
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
        return KEX.objects(
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
            secrets = self.redis_auth_operator
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
            secrets = self.redis_auth_operator
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

    @property
    def redis_auth_operator(self):
        try:
            secret = pykube.Secret.objects(
                self.api
            ).filter(
                namespace=self.namespace
            ).get(
                name="{0}-operator".format(self.name)
            )
            result = secret.obj['data']
            result = {
                "OperatorUsername": base64.b64decode(result["OperatorUsername"].encode()).decode(),
                "OperatorPassword": base64.b64decode(result["OperatorPassword"].encode()).decode(),
                "ReplUsername": base64.b64decode(result["ReplUsername"].encode()).decode(),
                "ReplPassword": base64.b64decode(result["ReplPassword"].encode()).decode(),
            }
        except pykube.exceptions.ObjectDoesNotExist:
            self.log.info(
                'Creating Secret {0}-operator'.format("{0}".format(self.name))
            )
            result = {
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
                    "OperatorUsername": base64.b64encode(result["OperatorUsername"].encode()).decode(),
                    "OperatorPassword": base64.b64encode(result["OperatorPassword"].encode()).decode(),
                    "ReplUsername": base64.b64encode(result["ReplUsername"].encode()).decode(),
                    "ReplPassword": base64.b64encode(result["ReplPassword"].encode()).decode(),
                }
            }
            kopf.adopt(secret_data)
            kopf.label(secret_data, {'RedisReplication': self.name})
            pykube.Secret(self.api, secret_data).create()
            self.log.info(
                'Creating Secret {0}-operator, done'.format("{0}".format(self.name))
            )
        return result

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
                self.spec['password']['secretName']
            ))
            raise RedisReplicationSecretMissing
        try:
            return base64.b64decode(secret.obj['data'][secret_data_key].encode()).decode()
        except KeyError:
            self.log.error('Secret {0} has no DataKey {1}'.format(
                self.spec['password']['secretName'],
                self.spec['password']['secretDataKey']
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
        self.log.info("deleting pod {0}".format(pod.name))
        pod.delete()
        self.redis.pop(pod.name, None)
        self.log.info("deleting pod {0}, done".format(pod.name))

    def pod_delete_candidate(self):
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
        while len(self.pods) < self.spec['replicas']:
            self.log.info("we have {0} of {1} replicas".format(
                len(self.pods),
                self.spec['replicas']
            ))
            self.pod_create()
            self.update_object_status()
        while len(self.pods) > self.spec['replicas']:
            self.log.info("we have {0} of {1} replicas".format(
                len(self.pods),
                self.spec['replicas']
            ))
            self.pod_delete(pod=self.pod_delete_candidate().pop())
            self.update_object_status()

    def pod_get_by_name(self, pod_name):
        return pykube.Pod.objects(
            self.api
        ).filter(
            namespace=self.namespace,
            selector={'RedisReplication': self.name}
        ).get(name=pod_name)

    def pod_is_ready(self, pod):
        self.log.info("checking if pod {0} is running".format(pod.name))
        self.log.info("pod {0} in {1} phase".format(
            pod.name, pod.obj['status']['phase']
        ))
        if pod.obj['status']['phase'] == 'Running':
            self.log.info("pod {0} is ready".format(pod.name))
            return pod
        if pod.obj['status']['phase'] == 'Failed':
            raise RedisReplicationPodError("pod in error state")
        if pod.obj['status']['phase'] == 'Succeeded':
            raise RedisReplicationPodError("pod in succeeded state")
        if pod.obj['status']['phase'] == 'Unknown':
            raise RedisReplicationPodError("pod in unknown state")
        raise RedisReplicationPodNotReady('pod {0} not ready.'.format(pod.name))

    def pod_set_label(self, pod, label_name, label_value, retry=3):
        self.log.info("setting label {0} with value {1} on pod {2}".format(
            label_name, label_value, pod.name
        ))
        while retry > 0:
            try:
                pod.labels[label_name] = str(label_value)
                pod.update()
                return
            except pykube.exceptions.HTTPError as err:
                self.log.warning("could not update pod {0}: {1}, retrying".format(pod.name, err))
                self.stopped.wait(1)
                retry -= 1
                pod = self.pod_get_by_name(pod_name=pod.name)

        raise RedisReplicationRedisConnError("could not update pod {0}, no more retires left".format(pod.name))

    def redis_client_connect(self, pod):
        self.log.info("connecting to redis {0}".format(pod))
        self.pod_is_ready(pod=pod)
        ip_addr = pod.obj['status']['podIP']
        try:
            if 'RedisReplicationOperatorACLPresent' not in pod.labels:
                self.redis_create_operator_acls(pod=pod, ip_addr=ip_addr)

            self.log.info("trying to login with redis-operator credentials")
            client = pyredis.Client(
                host=ip_addr,
                username=self.redis_auth_operator['OperatorUsername'],
                password=self.redis_auth_operator['OperatorPassword']
            )
            client.ping()
            self.redis[pod.name] = client
            self.log.info("trying to login with redis-operator credentials, success")
        except pyredis.exceptions.ReplyError as err:
            raise RedisReplicationRedisConnError(err)
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ) as err:
            self.log.debug(err)
            self.log.error("connection to pod {0} went away".format(pod.name))
            raise RedisReplicationRedisConnError("pod went away")
        self.log.info("connecting to redis {0}, done".format(pod))

    def redis_client_get(self, pod):
        if pod.name not in self.redis:
            self.redis_client_connect(pod=pod)
        return self.redis[pod.name]

    def redis_create_operator_acls(self, pod, ip_addr):
        self.log.info("trying to login without username/password")
        client = pyredis.Client(host=ip_addr)
        client.ping()
        self.log.info("trying to login without username/password, success")
        self.log.info("creating operator acl")
        self.redis_acl_enforce(
            pod=pod,
            username=self.redis_acl_operator.username,
            redis_acl=None,
            spec_acl=self.redis_acl_operator,
            client=client
        )
        self.log.info("creating operator acl, done")
        self.log.info("creating replication acl")
        self.redis_acl_enforce(
            pod=pod,
            username=self.redis_acl_repl.username,
            redis_acl=None,
            spec_acl=self.redis_acl_repl,
            client=client
        )
        self.log.info("creating replication acl, done")
        self.log.info("setting RedisReplicationOperatorACLPresent label")
        self.pod_set_label(
            pod=pod,
            label_name='RedisReplicationOperatorACLPresent',
            label_value=True
        )
        self.log.info("setting RedisReplicationOperatorACLPresent label, done")

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
                self.log.error("connection to pod {0} went away".format(pod.name))
                raise RedisReplicationRedisConnError("pod went away")
            target_value = self.redis_config_encode(value=target_value)
            if not current_value:
                self.log.warning("option {0} not found in redis, ignoring".format(option))
                continue
            if current_value[1] != target_value:
                self.log.info("option {0} current value {1} not matching {2}, adjusting".format(
                    option, current_value, target_value
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

    def redis_acl(self, pod):
        client = self.redis_client_get(pod=pod)

        try:
            for item in client.execute('INFO', 'REPLICATION').decode().split('\r\n'):
                if item.startswith('role:'):
                    role = item.split(':')[1]
                    self.log.info("{0} role is {1}".format(pod.name, role))
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ):
            self.log.error("connection to pod {0} went away".format(pod.name))
            raise RedisReplicationRedisConnError("pod went away")

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
            self.log.info("ACL changes for pod: {0} acl: {1} detected, executing: {2}".format(
                pod.name, username, command
            ))
            try:
                client.execute(*command)
            except (
                    pyredis.exceptions.PyRedisConnClosed,
                    pyredis.exceptions.PyRedisConnError,
                    pyredis.exceptions.PyRedisConnReadTimeout
            ) as err:
                self.log.error("connection to pod {0} went away: {1}".format(pod.name, err))
                raise RedisReplicationRedisConnError("pod went away")
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
                    self.log.info("deleting user {0}".format(username))
                    client.execute('ACL', 'DELUSER', username)
                    self.log.info("deleting user {0}, done".format(username))

    def redis_acl_list(self, pod):
        client = self.redis_client_get(pod=pod)
        try:
            _acls = client.execute('ACL', 'LIST')
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ):
            self.log.error("connection to pod {0} went away".format(pod.name))
            raise RedisReplicationRedisConnError("pod went away")
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
                self.log.error("lost connection to redis on pod {0}: {1}".format(
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
        self.log.info('promoting pod {0} to primary'.format(pod))
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
            self.log.info('promoting pod {0} to primary, success'.format(pod))
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ) as err:
            self.log.error("connection to pod {0} went away".format(pod.name))
            raise RedisReplicationRedisConnError(err)

    def redis_replication_secondary_enforce(self, pod):
        primary = self.pod_primary
        primary_name = self.pod_primary_name
        if not primary:
            self.log.warning("no primary detected, skipping secondary enforcement")
            return
        try:
            primary_ip = primary.obj['status']['podIP']
        except KeyError:
            self.log.warning("could not get PodIP from primary pod {0}".format(primary_name))
            return
        secrets = self.redis_auth_operator
        if pod.name == primary_name:
            return
        try:
            client = self.redis_client_get(pod=pod)
            replof = (client.execute('config', 'get', 'replicaof'))[1]
            replof = replof.decode()
            if replof.startswith(primary_ip):
                return
            self.log.info("making pod {0} replica of pod {1}".format(
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
            self.log.info("making pod {0} replica of pod {1}, success".format(
                pod.name, primary_name
            ))
        except (
                pyredis.exceptions.PyRedisConnClosed,
                pyredis.exceptions.PyRedisConnError,
                pyredis.exceptions.PyRedisConnReadTimeout
        ):
            self.log.error("connection to pod {0} went away".format(pod.name))
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
                    self.log.error("lost connection to redis on pod {0}: {1}".format(
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
