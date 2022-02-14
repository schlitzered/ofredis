import base64
import hashlib
import os
from unittest import TestCase
from unittest.mock import Mock, MagicMock, PropertyMock, call, patch
import uuid

import pykube
import pyredis.exceptions
import yaml

import ofredis
from ofredis import RedisReplicationErrorToManyPrimaries


class TestRedisReplicationUnit(TestCase):
    def setUp(self):
        self.data_path = "{0}/data".format(os.path.dirname(__file__))

        self.addCleanup(patch.stopall)

        kopf_patcher = patch('ofredis.kopf', autospec=True)
        self.mock_kopf = kopf_patcher.start()

        pykube_patcher = patch('ofredis.pykube', autospec=True)
        self.mock_pykube = pykube_patcher.start()
        self.mock_pykube.exceptions = pykube.exceptions

        self.mock_pykube_instance = Mock()
        self.mock_pykube.HTTPClient.return_value = self.mock_pykube_instance

        self.mock_pykube_pod_objects = Mock()
        self.mock_pykube.Pod.objects.return_value = self.mock_pykube_pod_objects

        pykube_rr_obj_patcher = patch('ofredis.PyKubeRedisReplication', autospec=True)
        self.mock_pykube_rr_obj = pykube_rr_obj_patcher.start()

        self.mock_pykube_rr_obj_objects = Mock()
        self.mock_pykube_rr_obj.objects.return_value = self.mock_pykube_rr_obj_objects

        self.mock_pykube_rr_obj_instance = Mock()
        self.mock_pykube_rr_obj_objects.get_by_name.return_value = self.mock_pykube_rr_obj_instance

        pyredis_patcher = patch('ofredis.pyredis', autospec=True)
        self.mock_pyredis = pyredis_patcher.start()
        self.mock_pyredis.exceptions = pyredis.exceptions

        self.mock_pykube_secret_objects = Mock()
        self.mock_pykube.Secret.objects.return_value = self.mock_pykube_secret_objects

        self.mock_stopped = Mock()
        self.mock_spec = dict()
        self.mock_logger = Mock()
        self.mock_name = 'dummy'
        self.mock_namespace = 'default'

        self.operator = ofredis.RedisReplication(
            stopped=self.mock_stopped,
            spec=self.mock_spec,
            logger=self.mock_logger,
            name=self.mock_name,
            namespace=self.mock_namespace,
        )

    def test__init__(self):
        pass

    def test_property_api(self):
        self.assertIs(self.operator.api, self.mock_pykube_instance)

    def test_property_log(self):
        self.assertIs(self.operator.log, self.mock_logger)

    def test_property_name(self):
        self.assertIs(self.operator.name, self.mock_name)

    def test_property_namespace(self):
        self.assertIs(self.operator.namespace, self.mock_namespace)

    def test_property_operator_status(self):
        self.assertIs(self.operator.operator_status, self.mock_pykube_rr_obj_instance)

    def test_property_pods(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        pod2 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        pod3 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        pod4 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default', 'deletionTimestamp': 12345}}
        )

        self.mock_pykube_pod_objects.filter.return_value = [
            pod1, pod2, pod3, pod4,
        ]

        self.assertEqual(
            self.operator.pods,
            [pod1, pod2, pod3]
        )
        self.mock_pykube_pod_objects.filter.assert_has_calls([
            call(
                namespace=self.operator.namespace,
                selector={'RedisReplication': self.operator.name}
            )
        ])

    def test_property_pod_primaries(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )

        self.mock_pykube_pod_objects.filter.return_value = [pod1]

        self.assertEqual(
            self.operator.pod_primaries,
            [pod1]
        )
        self.mock_pykube_pod_objects.filter.assert_has_calls([
            call(
                namespace=self.operator.namespace,
                selector={
                    'RedisReplication': self.operator.name,
                    'RedisReplicationRole': 'Primary'
                },
            )
        ])

    def test_property_pod_primary_zero(self):
        self.mock_pykube_pod_objects.filter.return_value = []
        self.assertIsNone(self.operator.pod_primary)

    def test_property_pod_primary_one(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        self.mock_pykube_pod_objects.filter.return_value = [pod1]
        self.assertEqual(self.operator.pod_primary, pod1)

    def test_property_pod_primary_many(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        pod2 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod2', 'namespace': 'default'}}
        )
        self.mock_pykube_pod_objects.filter.return_value = [pod1, pod2]

        def wrap_property():
            return self.operator.pod_primary

        self.assertRaises(RedisReplicationErrorToManyPrimaries, wrap_property)

    def test_property_pod_primary_name(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        self.mock_pykube_pod_objects.filter.return_value = [pod1]
        self.assertEqual(self.operator.pod_primary_name, 'pod1')

    def test_property_pod_primary_name_none(self):
        self.mock_pykube_pod_objects.filter.return_value = []
        self.assertIsNone(self.operator.pod_primary_name)

    def test_property_pod_secondaries(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )

        self.mock_pykube_pod_objects.filter.return_value = [pod1]

        self.assertEqual(
            self.operator.pod_secondaries,
            [pod1]
        )
        self.mock_pykube_pod_objects.filter.assert_has_calls([
            call(
                namespace=self.operator.namespace,
                selector={
                    'RedisReplication': self.operator.name,
                    'RedisReplicationRole': 'Secondary'
                },
            )
        ])

    def test_property_redis_acl_operator(self):
        self.operator._redis_operator_secrets = {
            "OperatorUsername": str(uuid.uuid4()),
            "OperatorPassword": str(uuid.uuid4()),
        }
        acl = self.operator.redis_acl_operator
        self.assertIsInstance(acl, ofredis.RedisAcl)
        self.assertEqual(acl.log, self.mock_logger)
        self.assertEqual(
            acl.username, self.operator.redis_operator_secrets['OperatorUsername']
        )
        self.assertEqual(acl.user_enable, 'on')
        self.assertEqual(
            acl.passwords,
            {
                "#{0}".format(
                    hashlib.sha256(
                        self.operator.redis_operator_secrets['OperatorPassword'].encode()
                    ).hexdigest()
                )
            }
        )
        self.assertEqual(
            acl.commands,
            {'+acl', '+config', '+info', '-@all', '+ping', '+replicaof'}
        )

    def test_property_redis_acl_repl(self):
        self.operator._redis_operator_secrets = {
            "ReplUsername": str(uuid.uuid4()),
            "ReplPassword": str(uuid.uuid4()),
        }
        acl = self.operator.redis_acl_repl
        self.assertIsInstance(acl, ofredis.RedisAcl)
        self.assertEqual(acl.log, self.mock_logger)
        self.assertEqual(
            acl.username, self.operator.redis_operator_secrets['ReplUsername']
        )
        self.assertEqual(acl.user_enable, 'on')
        self.assertEqual(
            acl.passwords,
            {
                "#{0}".format(
                    hashlib.sha256(
                        self.operator.redis_operator_secrets['ReplPassword'].encode()
                    ).hexdigest()
                )
            }
        )
        self.assertEqual(
            acl.commands,
            {'+psync', '+replconf', '+ping', '-@all'}
        )

    def test_property_redis_operator_secrets_get(self):
        secrets = {
            "OperatorUsername": str(uuid.uuid4()),
            "OperatorPassword": str(uuid.uuid4()),
            "ReplUsername": str(uuid.uuid4()),
            "ReplPassword": str(uuid.uuid4()),
        }
        api_secrets = pykube.Secret(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': '{0}-operator'.format(self.operator.name),
                    'namespace': 'default'
                },
                'data': {
                    "OperatorUsername": base64.b64encode(secrets["OperatorUsername"].encode()).decode(),
                    "OperatorPassword": base64.b64encode(secrets["OperatorPassword"].encode()).decode(),
                    "ReplUsername": base64.b64encode(secrets["ReplUsername"].encode()).decode(),
                    "ReplPassword": base64.b64encode(secrets["ReplPassword"].encode()).decode(),
                }
            }
        )
        mock_filter_get = Mock()
        mock_filter_get.get.return_value = api_secrets
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get
        self.assertEqual(secrets, self.operator.redis_operator_secrets)

    def test_property_redis_operator_secrets_create(self):
        mock_filter_get = Mock()
        mock_filter_get.get.side_effect = pykube.exceptions.ObjectDoesNotExist()
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get
        secrets = self.operator.redis_operator_secrets
        self.assertIn('OperatorPassword', secrets)
        self.assertIn('OperatorUsername', secrets)
        self.assertIn('ReplPassword', secrets)
        self.assertIn('ReplUsername', secrets)
        self.assertIsInstance(secrets['OperatorPassword'], str)
        self.assertIsInstance(secrets['OperatorUsername'], str)
        self.assertIsInstance(secrets['ReplPassword'], str)
        self.assertIsInstance(secrets['ReplUsername'], str)

    def test_property_redis_operator_secrets_retry(self):
        mock_filter_get = Mock()
        mock_filter_get.get.side_effect = pykube.exceptions.ObjectDoesNotExist()
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get
        mock_create = Mock()
        mock_create.create.side_effect = [
            pykube.exceptions.KubernetesError(),
            True
        ]
        self.mock_pykube.Secret.return_value = mock_create
        self.assertIsInstance(self.operator.redis_operator_secrets, dict)

    def test_property_redis(self):
        self.assertIsInstance(self.operator.redis, dict)

    def test_property_spec(self):
        self.assertIsInstance(self.operator.spec, dict)

    def test_property_spec_acls_password_nopass(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].passwords, {'nopass'})

    def test_property_spec_acls_password_clear_text(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'dummy2'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].passwords, {'#d6f175817f886ec6fbbc1515326465fa96c3bfd54a4ea06cfd6dbbd8340e0152'})
        self.assertEqual(spec_acls['dummy2'].commands, {'-@all'})

    def test_property_spec_acls_password_sha256(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = '72f33270634cb329c8c27b90e6378330dfa147bd8d2f20f05b7419bf1a5b8cbd'
        spec_acls = self.operator.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].passwords, {'#72f33270634cb329c8c27b90e6378330dfa147bd8d2f20f05b7419bf1a5b8cbd'})

    def test_property_spec_acls_user_enable(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'dummy1'
        spec_acls = self.operator.spec_acls

        self.assertEqual(spec_acls['dummy1'].user_enable, 'on')

    def test_property_spec_acls_user_disable(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'dummy4'
        spec_acls = self.operator.spec_acls

        self.assertIn('dummy4', spec_acls)
        self.assertEqual(spec_acls['dummy4'].passwords, {'#6c4386f00ebd38905e615e5bde0d39ed8f29c675a9eaaf2700b465564a1dd071'})
        self.assertEqual(spec_acls['dummy4'].user_enable, 'off')

    def test_property_spec_acls_commands_all(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].commands, {'+@all'})

    def test_property_spec_acls_commands_none(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].commands, {'-@all'})

    def test_property_spec_acls_commands_none_fallback(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy4', spec_acls)
        self.assertEqual(spec_acls['dummy4'].commands, {'-@all'})

    def test_property_spec_acls_commands_selected(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].commands, {'-@all', 'get', 'set'})

    def test_property_spec_acls_key_patterns_all(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].key_patterns, {'~*'})

    def test_property_spec_acls_key_patterns_none(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].key_patterns, set())

    def test_property_spec_acls_key_patterns_selected(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].key_patterns, {'~dummy3*'})

    def test_property_spec_acls_pubsub_patterns_all(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].pubsub_patterns, {'&*'})

    def test_property_spec_acls_pubsub_patterns_none(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].pubsub_patterns, set())

    def test_property_spec_acls_pubsub_patterns_selected(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].pubsub_patterns, {'&dummy3*'})

    def test_property_spec_acls_filter_redis_operator(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertNotIn('RedisOperator', spec_acls)

    def test_property_spec_acls_filter_redis_repl(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        self.operator.password = Mock()
        self.operator.password.return_value = 'nopass'

        spec_acls = self.operator.spec_acls

        self.assertNotIn('RedisRepl', spec_acls)

    def test_passwort(self):
        secrets = {
            "Password": str(uuid.uuid4()),
        }
        api_secrets = pykube.Secret(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': '{0}-operator'.format(self.operator.name),
                    'namespace': 'default'
                },
                'data': {
                    "Password": base64.b64encode(secrets["Password"].encode()).decode(),
                }
            }
        )
        mock_filter_get = Mock()
        mock_filter_get.get.return_value = api_secrets
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get

        self.assertEqual(
            self.operator.password(
                secret_name='secret_name',
                secret_data_key='Password'
            ),
            secrets['Password']
        )

        self.mock_pykube_secret_objects.filter.assert_has_calls([
            call(
                namespace=self.operator.namespace,
            )
        ])

        mock_filter_get.get.assert_has_calls([
            call(
                name='secret_name',
            )
        ])

    def test_passwort_raise_secret_missing(self):
        mock_filter_get = Mock()
        mock_filter_get.get.side_effect = pykube.exceptions.ObjectDoesNotExist()
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get

        self.assertRaises(
            ofredis.RedisReplicationSecretMissing,
            self.operator.password,
            secret_name='secret_name',
            secret_data_key='Password'
        )

    def test_passwort_raise_missing_data_key(self):
        api_secrets = pykube.Secret(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': '{0}-operator'.format(self.operator.name),
                    'namespace': 'default'
                },
                'data': {
                }
            }
        )
        mock_filter_get = Mock()
        mock_filter_get.get.return_value = api_secrets
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get

        self.assertRaises(
            ofredis.RedisReplicationSecretMissing,
            self.operator.password,
            secret_name='secret_name',
            secret_data_key='Password'
        )

    def test_pod_create(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']

        pod_data = {
            "apiVersion": "v1",
            "kind": "Pod",
            "spec": {
                "restartPolicy": 'Never',
                "containers": [
                    {
                        "name": self.operator.name,
                        "image": self.operator.spec['redis']['image'],
                        "ports": [
                            {
                                "containerPort": 6379
                            }
                        ]
                    }
                ]
            }
        }
        mock_create = Mock()
        self.mock_pykube.Pod.return_value = mock_create

        self.operator.pod_create()

        self.mock_kopf.adopt.assert_called_with(pod_data)
        self.mock_kopf.label.assert_called_with(
            pod_data, {'RedisReplication': self.operator.name}
        )

        self.mock_pykube.Pod.assert_called_with(
            self.operator.api, pod_data
        )

        mock_create.create.assert_called()

    def test_pod_delete(self):
        dummy_pod = Mock()
        dummy_pod.name = 'dummy_pod'
        self.operator.redis['dummy_pod'] = dummy_pod
        self.operator.pod_delete(dummy_pod)

        self.assertNotIn('dummy_pod', self.operator.redis)
        dummy_pod.delete.assert_called()

    def test_pod_delete_candidates_unconfigured(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )

        pod2 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod2', 'namespace': 'default'}}
        )

        pod3 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod3',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        self.mock_pykube_pod_objects.filter.return_value = [
            pod1, pod2, pod3
        ]

        candidates = self.operator.pod_delete_candidates()
        self.assertEqual(candidates, [pod1, pod2])

    def test_pod_delete_candidates_secondaries(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod2 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod2',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod3 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod3',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        self.mock_pykube_pod_objects.filter.return_value = [
            pod1, pod2, pod3
        ]

        candidates = self.operator.pod_delete_candidates()
        self.assertEqual(candidates, [pod1, pod2, pod3])

    def test_pod_ensure_count_nothing_todo(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod2 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod2',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod3 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod3',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        self.mock_pykube_pod_objects.filter.return_value = [pod1, pod2, pod3]

        self.operator.pod_create = Mock()
        self.operator.pod_delete = Mock()
        self.operator.update_object_status = Mock()

        self.operator.pod_ensure_count()

        self.assertFalse(self.operator.pod_create.called)
        self.assertFalse(self.operator.pod_delete.called)
        self.assertFalse(self.operator.update_object_status.called)

    def test_pod_ensure_count_two_missing(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        self.mock_pykube_pod_objects.filter.return_value = [pod1]

        self.operator.pod_create = Mock()
        self.operator.pod_delete = Mock()
        self.operator.update_object_status = Mock()

        self.operator.pod_ensure_count()

        self.assertEqual(2, self.operator.pod_create.call_count)
        self.assertEqual(2, self.operator.update_object_status.call_count)

        self.assertFalse(self.operator.pod_delete.called)

    def test_pod_ensure_count_two_to_much(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator._spec = spec['spec']
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod2 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod2',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod3 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod3',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod4 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod4',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        pod5 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod5',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )

        self.mock_pykube_pod_objects.filter.return_value = [pod1, pod2, pod3, pod4, pod5]

        self.operator.pod_create = Mock()
        self.operator.pod_delete = Mock()
        self.operator.pod_delete_candidates = Mock()
        self.operator.pod_delete_candidates.side_effect = [
            [pod1, pod2, pod3, pod4, pod5],
            [pod1, pod2, pod3, pod4],
        ]
        self.operator.update_object_status = Mock()

        self.operator.pod_ensure_count()

        self.operator.pod_delete.assert_has_calls([
            call(pod=pod5),
            call(pod=pod4),
        ])
        self.assertEqual(2, self.operator.update_object_status.call_count)
        self.assertEqual(2, self.operator.pod_delete_candidates.call_count)

        self.assertFalse(self.operator.pod_create.called)

    def test_pod_get_by_name(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
            }
        )
        mock_filter_get = Mock()
        mock_filter_get.get.return_value = pod1
        self.mock_pykube_pod_objects.filter.return_value = mock_filter_get

        self.assertEqual(self.operator.pod_get_by_name(pod_name='pod1'), pod1)

        self.mock_pykube_pod_objects.filter.assert_has_calls([
            call(
                namespace=self.operator.namespace,
                selector={'RedisReplication': self.operator.name}
            )
        ])

        mock_filter_get.get.assert_has_calls([
            call(name='pod1')
        ])

    def test_pod_is_ready_running(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Running'
                }
            }
        )

        self.assertEqual(self.operator.pod_is_ready(pod=pod1), pod1)

    def test_pod_is_ready_pending(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Pending'
                }
            }
        )

        self.assertRaises(
            ofredis.RedisReplicationPodNotReady, self.operator.pod_is_ready, pod=pod1
        )

    def test_pod_is_ready_failed(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Failed'
                }
            }
        )

        self.assertRaises(
            ofredis.RedisReplicationPodError, self.operator.pod_is_ready, pod=pod1
        )

    def test_pod_is_ready_succeeded(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Succeeded'
                }
            }
        )

        self.assertRaises(
            ofredis.RedisReplicationPodError, self.operator.pod_is_ready, pod=pod1
        )

    def test_pod_is_ready_unknown(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Unknown'
                }
            }
        )

        self.assertRaises(
            ofredis.RedisReplicationPodError, self.operator.pod_is_ready, pod=pod1
        )

    def test_pod_is_ready_real_unknown_state(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'WhatTheHeck'
                }
            }
        )

        self.assertRaises(
            ofredis.RedisReplicationPodError, self.operator.pod_is_ready, pod=pod1
        )

    def test_pod_set_label(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Unknown'
                }
            }
        )
        update_mock = Mock()
        pod1.update = update_mock

        self.operator.pod_set_label(
            pod=pod1,
            label_name='dummy_label',
            label_value='dummy_value'
        )

        update_mock.assert_called()

        self.assertEqual(pod1.labels['dummy_label'], 'dummy_value')

    def test_pod_set_label_retry(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Unknown'
                }
            }
        )
        update_mock = Mock()
        update_mock.side_effect = [
            pykube.exceptions.HTTPError(code=500, message='some error'),
            pykube.exceptions.HTTPError(code=500, message='some error'),
            True
        ]
        pod1.update = update_mock

        mock_filter_get = Mock()
        mock_filter_get.get.return_value = pod1
        self.mock_pykube_pod_objects.filter.return_value = mock_filter_get

        self.operator.pod_set_label(
            pod=pod1,
            label_name='dummy_label',
            label_value='dummy_value'
        )

        update_mock.assert_called()

        self.assertEqual(pod1.labels['dummy_label'], 'dummy_value')

    def test_pod_set_label_retry_exceeded(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {'RedisReplicationRole': 'Secondary'}
                },
                'status': {
                    'phase': 'Unknown'
                }
            }
        )
        update_mock = Mock()
        update_mock.side_effect = [
            pykube.exceptions.HTTPError(code=500, message='some error'),
            pykube.exceptions.HTTPError(code=500, message='some error'),
            pykube.exceptions.HTTPError(code=500, message='some error'),
        ]
        pod1.update = update_mock

        mock_filter_get = Mock()
        mock_filter_get.get.return_value = pod1
        self.mock_pykube_pod_objects.filter.return_value = mock_filter_get

        self.assertRaises(
            ofredis.RedisReplicationRedisConnError,
            self.operator.pod_set_label,
            pod=pod1,
            label_name='dummy_label',
            label_value='dummy_value'

        )

    def test_redis_client_connect(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {
                        'RedisReplicationRole': 'Secondary',
                        'RedisReplicationOperatorACLPresent': True
                    }
                },
                'status': {
                    'podIP': '10.0.0.1',
                    'phase': 'Running'
                }
            }
        )
        self.operator._redis_operator_secrets = {
            'OperatorUsername': 'operator_user',
            'OperatorPassword': 'operator_password'
        }

        client_mock = Mock()
        self.mock_pyredis.Client.return_value = client_mock
        self.operator.redis_client_connect(pod=pod1)

        self.mock_pyredis.Client.assert_called_with(
            host='10.0.0.1',
            username='operator_user',
            password='operator_password'
        )
        self.assertTrue(client_mock.ping.called)

        self.assertEqual(client_mock, self.operator.redis['pod1'])

    def test_redis_client_connect_create_operator_acls(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {
                        'RedisReplicationRole': 'Secondary',
                    }
                },
                'status': {
                    'podIP': '10.0.0.1',
                    'phase': 'Running'
                }
            }
        )
        self.operator._redis_operator_secrets = {
            'OperatorUsername': 'operator_user',
            'OperatorPassword': 'operator_password',
            'ReplUsername': 'repl_user',
            'ReplPassword': 'repl_password'
        }

        client_mock_no_acl = Mock()
        client_mock_has_acl = Mock()
        self.mock_pyredis.Client.side_effect = [
            client_mock_no_acl,
            client_mock_has_acl
        ]

        redis_acl_enforce_mock = Mock()
        self.operator.redis_acl_enforce = redis_acl_enforce_mock

        pod_set_label_mock = Mock()
        self.operator.pod_set_label = pod_set_label_mock

        self.operator.redis_client_connect(pod=pod1)

        self.mock_pyredis.Client.assert_has_calls([
            call(
                host='10.0.0.1',
            ),
            call(
                host='10.0.0.1',
                username='operator_user',
                password='operator_password'
            )
        ])
        self.assertTrue(client_mock_no_acl.ping.called)

        redis_acl_enforce_mock.assert_has_calls([
            call(
                pod=pod1,
                username='operator_user',
                redis_acl=None,
                spec_acl=self.operator.redis_acl_operator,
                client=client_mock_no_acl
            ),
            call(
                pod=pod1,
                username='repl_user',
                redis_acl=None,
                spec_acl=self.operator.redis_acl_repl,
                client=client_mock_no_acl
            ),
        ])

        pod_set_label_mock.assert_called_with(
            pod=pod1,
            label_name='RedisReplicationOperatorACLPresent',
            label_value=True
        )

        self.assertTrue(client_mock_has_acl.ping.called)
        self.assertEqual(client_mock_has_acl, self.operator.redis['pod1'])

    def test_redis_client_connect_redis_reply_error(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {
                        'RedisReplicationRole': 'Secondary',
                        'RedisReplicationOperatorACLPresent': True
                    }
                },
                'status': {
                    'podIP': '10.0.0.1',
                    'phase': 'Running'
                }
            }
        )
        self.operator._redis_operator_secrets = {
            'OperatorUsername': 'operator_user',
            'OperatorPassword': 'operator_password'
        }

        client_mock = Mock()
        client_mock.ping.side_effect = pyredis.exceptions.ReplyError()
        self.mock_pyredis.Client.return_value = client_mock
        self.assertRaises(
            ofredis.RedisReplicationRedisConnError,
            self.operator.redis_client_connect,
            pod=pod1
        )

        self.mock_pyredis.Client.assert_called_with(
            host='10.0.0.1',
            username='operator_user',
            password='operator_password'
        )
        self.assertTrue(client_mock.ping.called)

    def test_redis_client_get(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {
                        'RedisReplicationRole': 'Secondary',
                        'RedisReplicationOperatorACLPresent': True
                    }
                },
                'status': {
                    'podIP': '10.0.0.1',
                    'phase': 'Running'
                }
            }
        )

        self.operator.redis['pod1'] = pod1

        self.assertEqual(pod1, self.operator.redis_client_get(pod=pod1))

    def test_redis_client_get_create(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod1',
                    'namespace': 'default',
                    'labels': {
                        'RedisReplicationRole': 'Secondary',
                        'RedisReplicationOperatorACLPresent': True
                    }
                },
                'status': {
                    'podIP': '10.0.0.1',
                    'phase': 'Running'
                }
            }
        )

        def redis_client_connect(pod):
            self.operator.redis['pod1'] = pod1

        self.operator.redis_client_connect = redis_client_connect
        self.assertEqual(pod1, self.operator.redis_client_get(pod=pod1))

