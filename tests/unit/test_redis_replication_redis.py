import base64
import hashlib
import uuid
import unittest.mock
from unittest.mock import Mock, PropertyMock, call

import pykube
import pyredis.exceptions
import yaml

from tests.unit.base import TestRedisReplicationUnitBase

import ofredis


class TestRedisAclsEnforce(TestRedisReplicationUnitBase):
    def setUp(self):
        super().setUp()

        self.mock_pykube_secret_objects = Mock()
        self.mock_pykube.Secret.objects.return_value = self.mock_pykube_secret_objects

        self.pod_secondary = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod_secondary',
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

        self.pod_unconfigured = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={
                'metadata': {
                    'name': 'pod_unconfigured',
                    'namespace': 'default',
                    'labels': {
                    }
                },
                'status': {
                    'podIP': '10.0.0.1',
                    'phase': 'Running'
                }
            }
        )
        redis_client_mock = Mock()
        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.redis_acl_mock = Mock()
        self.operator.acl_list = redis_client_mock

        self.spec_acls_mock = unittest.mock.patch('ofredis.RedisReplication.spec_acls', new_callable=PropertyMock)

    def test_property_redis_acl_operator(self):
        self.operator.redis._operator_secrets = {
            "OperatorUsername": str(uuid.uuid4()),
            "OperatorPassword": str(uuid.uuid4()),
        }
        acl = self.operator.redis.acl_operator
        self.assertIsInstance(acl, ofredis.RedisAcl)
        self.assertEqual(acl.log, self.mock_logger)
        self.assertEqual(
            acl.username, self.operator.redis.operator_secrets['OperatorUsername']
        )
        self.assertEqual(acl.user_enable, 'on')
        self.assertEqual(
            acl.passwords,
            {
                "#{0}".format(
                    hashlib.sha256(
                        self.operator.redis.operator_secrets['OperatorPassword'].encode()
                    ).hexdigest()
                )
            }
        )
        self.assertEqual(
            acl.commands,
            {'+acl', '+config', '+info', '-@all', '+ping', '+replicaof'}
        )

    def test_property_redis_acl_repl(self):
        self.operator.redis._operator_secrets = {
            "ReplUsername": str(uuid.uuid4()),
            "ReplPassword": str(uuid.uuid4()),
        }
        acl = self.operator.redis.acl_repl
        self.assertIsInstance(acl, ofredis.RedisAcl)
        self.assertEqual(acl.log, self.mock_logger)
        self.assertEqual(
            acl.username, self.operator.redis.operator_secrets['ReplUsername']
        )
        self.assertEqual(acl.user_enable, 'on')
        self.assertEqual(
            acl.passwords,
            {
                "#{0}".format(
                    hashlib.sha256(
                        self.operator.redis.operator_secrets['ReplPassword'].encode()
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
        self.assertEqual(secrets, self.operator.redis.operator_secrets)

    def test_property_redis_operator_secrets_create(self):
        mock_filter_get = Mock()
        mock_filter_get.get.side_effect = pykube.exceptions.ObjectDoesNotExist()
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get
        secret_create_mock = Mock()
        self.mock_pykube.Secret.return_value = secret_create_mock

        secrets = self.operator.redis.operator_secrets

        self.assertIn('OperatorPassword', secrets)
        self.assertIn('OperatorUsername', secrets)
        self.assertIn('ReplPassword', secrets)
        self.assertIn('ReplUsername', secrets)
        self.assertIsInstance(secrets['OperatorPassword'], str)
        self.assertIsInstance(secrets['OperatorUsername'], str)
        self.assertIsInstance(secrets['ReplPassword'], str)
        self.assertIsInstance(secrets['ReplUsername'], str)

        secret_data = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "{0}-operator".format(self.operator.redis.name)},
            "type": "Opaque",
            "data": {
                "OperatorUsername": base64.b64encode(secrets["OperatorUsername"].encode()).decode(),
                "OperatorPassword": base64.b64encode(secrets["OperatorPassword"].encode()).decode(),
                "ReplUsername": base64.b64encode(secrets["ReplUsername"].encode()).decode(),
                "ReplPassword": base64.b64encode(secrets["ReplPassword"].encode()).decode(),
            }
        }

        self.mock_pykube.Secret.assert_called_with(
            self.operator.redis.api,
            secret_data

        )

    def test_property_redis_operator_secrets_retry(self):
        mock_filter_get = Mock()
        mock_filter_get.get.side_effect = pykube.exceptions.ObjectDoesNotExist()
        self.mock_pykube_secret_objects.filter.return_value = mock_filter_get
        secret_create_mock = Mock()
        secret_create_mock.create.side_effect = [
            pykube.exceptions.KubernetesError(),
            True
        ]
        self.mock_pykube.Secret.return_value = secret_create_mock
        secrets = self.operator.redis.operator_secrets

        self.assertIsInstance(secrets, dict)

        self.assertIn('OperatorPassword', secrets)
        self.assertIn('OperatorUsername', secrets)
        self.assertIn('ReplPassword', secrets)
        self.assertIn('ReplUsername', secrets)
        self.assertIsInstance(secrets['OperatorPassword'], str)
        self.assertIsInstance(secrets['OperatorUsername'], str)
        self.assertIsInstance(secrets['ReplPassword'], str)
        self.assertIsInstance(secrets['ReplUsername'], str)

        secret_data = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "{0}-operator".format(self.operator.redis.name)},
            "type": "Opaque",
            "data": {
                "OperatorUsername": base64.b64encode(secrets["OperatorUsername"].encode()).decode(),
                "OperatorPassword": base64.b64encode(secrets["OperatorPassword"].encode()).decode(),
                "ReplUsername": base64.b64encode(secrets["ReplUsername"].encode()).decode(),
                "ReplPassword": base64.b64encode(secrets["ReplPassword"].encode()).decode(),
            }
        }

        self.mock_pykube.Secret.assert_called_with(
            self.operator.redis.api,
            secret_data

        )

    def test_property_redis_connections(self):
        self.assertIsInstance(self.operator.redis.connections, dict)

    def test_property_spec_acls_password_nopass(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].passwords, {'nopass'})

    def test_property_spec_acls_password_clear_text(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'dummy2'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].passwords, {'#d6f175817f886ec6fbbc1515326465fa96c3bfd54a4ea06cfd6dbbd8340e0152'})
        self.assertEqual(spec_acls['dummy2'].commands, {'-@all'})

    def test_property_spec_acls_password_sha256(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = '72f33270634cb329c8c27b90e6378330dfa147bd8d2f20f05b7419bf1a5b8cbd'
        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].passwords, {'#72f33270634cb329c8c27b90e6378330dfa147bd8d2f20f05b7419bf1a5b8cbd'})

    def test_property_spec_acls_user_enable(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'dummy1'
        spec_acls = self.operator.redis.spec_acls

        self.assertEqual(spec_acls['dummy1'].user_enable, 'on')

    def test_property_spec_acls_user_disable(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'dummy4'
        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy4', spec_acls)
        self.assertEqual(spec_acls['dummy4'].passwords, {'#6c4386f00ebd38905e615e5bde0d39ed8f29c675a9eaaf2700b465564a1dd071'})
        self.assertEqual(spec_acls['dummy4'].user_enable, 'off')

    def test_property_spec_acls_commands_all(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].commands, {'+@all'})

    def test_property_spec_acls_commands_none(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].commands, {'-@all'})

    def test_property_spec_acls_commands_none_fallback(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy4', spec_acls)
        self.assertEqual(spec_acls['dummy4'].commands, {'-@all'})

    def test_property_spec_acls_commands_selected(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].commands, {'-@all', 'get', 'set'})

    def test_property_spec_acls_key_patterns_all(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].key_patterns, {'~*'})

    def test_property_spec_acls_key_patterns_none(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].key_patterns, set())

    def test_property_spec_acls_key_patterns_selected(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].key_patterns, {'~dummy3*'})

    def test_property_spec_acls_pubsub_patterns_all(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy1', spec_acls)
        self.assertEqual(spec_acls['dummy1'].pubsub_patterns, {'&*'})

    def test_property_spec_acls_pubsub_patterns_none(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy2', spec_acls)
        self.assertEqual(spec_acls['dummy2'].pubsub_patterns, set())

    def test_property_spec_acls_pubsub_patterns_selected(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertIn('dummy3', spec_acls)
        self.assertEqual(spec_acls['dummy3'].pubsub_patterns, {'&dummy3*'})

    def test_property_spec_acls_filter_redis_operator(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertNotIn('RedisOperator', spec_acls)

    def test_property_spec_acls_filter_redis_repl(self):
        with open('{0}/test_property_spec_acls.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']
        self.operator.redis.password = Mock()
        self.operator.redis.password.return_value = 'nopass'

        spec_acls = self.operator.redis.spec_acls

        self.assertNotIn('RedisRe.redispl', spec_acls)

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
            self.operator.redis.password(
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
            self.operator.redis.password,
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
            self.operator.redis.password,
            secret_name='secret_name',
            secret_data_key='Password'
        )

    def test_redis_client_connect(self):
        self.operator.redis._operator_secrets = {
            'OperatorUsername': 'operator_user',
            'OperatorPassword': 'operator_password'
        }

        client_mock = Mock()
        self.mock_pyredis.Client.return_value = client_mock
        self.operator.redis.client_connect(pod=self.pod_secondary)

        self.mock_pyredis.Client.assert_called_with(
            host='10.0.0.1',
            username='operator_user',
            password='operator_password'
        )
        self.assertTrue(client_mock.ping.called)

        self.assertEqual(client_mock, self.operator.redis.connections['pod_secondary'])

    def test_redis_client_connect_create_operator_acls(self):
        self.operator.redis._operator_secrets = {
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
        self.operator.redis.acl_enforce = redis_acl_enforce_mock

        pod_set_label_mock = Mock()
        self.operator.pod.set_label = pod_set_label_mock

        self.operator.redis.client_connect(pod=self.pod_unconfigured)

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
                pod=self.pod_unconfigured,
                username='operator_user',
                redis_acl=None,
                spec_acl=self.operator.redis.acl_operator,
                client=client_mock_no_acl
            ),
            call(
                pod=self.pod_unconfigured,
                username='repl_user',
                redis_acl=None,
                spec_acl=self.operator.redis.acl_repl,
                client=client_mock_no_acl
            ),
        ])

        pod_set_label_mock.assert_called_with(
            pod=self.pod_unconfigured,
            label_name='RedisReplicationOperatorACLPresent',
            label_value=True
        )

        self.assertTrue(client_mock_has_acl.ping.called)
        self.assertEqual(client_mock_has_acl, self.operator.redis.connections['pod_unconfigured'])

    def test_redis_client_connect_redis_reply_error(self):
        self.operator.redis._operator_secrets = {
            'OperatorUsername': 'operator_user',
            'OperatorPassword': 'operator_password'
        }

        client_mock = Mock()
        client_mock.ping.side_effect = pyredis.exceptions.ReplyError()
        self.mock_pyredis.Client.return_value = client_mock
        self.assertRaises(
            ofredis.RedisReplicationRedisConnError,
            self.operator.redis.client_connect,
            pod=self.pod_secondary
        )

        self.mock_pyredis.Client.assert_called_with(
            host='10.0.0.1',
            username='operator_user',
            password='operator_password'
        )
        self.assertTrue(client_mock.ping.called)

    def test_redis_client_get(self):
        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.assertEqual(redis_client_mock, self.operator.redis.client_get(pod=self.pod_secondary))

    def test_redis_client_get_create(self):
        self.operator.redis.connections.pop('pod_secondary')
        redis_client_mock = Mock()

        def redis_client_connect(pod):
            self.operator.redis.connections[pod.name] = redis_client_mock

        self.operator.redis.client_connect = redis_client_connect
        self.assertEqual(redis_client_mock, self.operator.redis.client_get(pod=self.pod_secondary))

    def test_redis_config_enforce_unit_convert(self):
        with open('{0}/test_redis_config_enforce_unit_convert.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']

        redis_client_mock = Mock()
        redis_client_mock.execute.side_effect = [
            [b'maxmemory', b'0'],
            b'OK',
            [b'maxmemory', b'0'],
            b'OK',
            [b'maxmemory', b'0'],
            b'OK',
            [b'maxmemory', b'0'],
            b'OK',
            [b'maxmemory', b'0'],
            b'OK',
            [b'maxmemory', b'0'],
            b'OK',
        ]

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.assertEqual(redis_client_mock, self.operator.redis.client_get(pod=self.pod_secondary))

        self.operator.redis.config_enforce(pod=self.pod_secondary)

        redis_client_mock.execute.assert_has_calls([
            call('CONFIG', 'GET', 'maxmemory_1'),
            call('CONFIG', 'SET', 'maxmemory_1', b'128000'),
            call('CONFIG', 'GET', 'maxmemory_2'),
            call('CONFIG', 'SET', 'maxmemory_2', b'131072'),
            call('CONFIG', 'GET', 'maxmemory_3'),
            call('CONFIG', 'SET', 'maxmemory_3', b'128000000'),
            call('CONFIG', 'GET', 'maxmemory_4'),
            call('CONFIG', 'SET', 'maxmemory_4', b'134217728'),
            call('CONFIG', 'GET', 'maxmemory_5'),
            call('CONFIG', 'SET', 'maxmemory_5', b'128000000000'),
            call('CONFIG', 'GET', 'maxmemory_6'),
            call('CONFIG', 'SET', 'maxmemory_6', b'137438953472'),
        ])

    def test_redis_config_enforce_filter_config_options(self):
        with open('{0}/test_redis_config_enforce_filter_config_options.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.assertEqual(redis_client_mock, self.operator.redis.client_get(pod=self.pod_secondary))

        self.operator.redis.config_enforce(pod=self.pod_secondary)

        self.assertFalse(redis_client_mock.execute.called)

    def test_redis_config_enforce_unknown_option(self):
        with open('{0}/test_redis_config_enforce_unknown_option.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']

        redis_client_mock = Mock()
        redis_client_mock.execute.return_value = []

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.assertEqual(redis_client_mock, self.operator.redis.client_get(pod=self.pod_secondary))

        self.operator.redis.config_enforce(pod=self.pod_secondary)

    def test_redis_config_enforce_redis_exception(self):
        with open('{0}/test_redis_config_enforce_unknown_option.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.redis._spec = spec['spec']

        redis_client_mock = Mock()
        redis_client_mock.execute.side_effect = [
            pyredis.exceptions.PyRedisConnClosed()
        ]

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.assertEqual(redis_client_mock, self.operator.redis.client_get(pod=self.pod_secondary))

        self.assertRaises(
            ofredis.RedisReplicationRedisConnError,
            self.operator.redis.config_enforce,
            pod=self.pod_secondary
        )

    def test_redis_acl_enforce_no_diff(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on'
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

        self.assertFalse(redis_client_mock.execute.called)

    def test_redis_acl_enforce_no_redis_acl(self):
        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on'
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=None,
            spec_acl=spec_acl,
        )

        redis_client_mock.execute.assert_called_with(
            'ACL', 'SETUSER', 'testuser', 'on', '-@all', 'resetpass', 'resetchannels'
        )

    def test_redis_acl_enforce_diff_user_enable(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='off'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on'
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

        redis_client_mock.execute.assert_called_with('ACL', 'SETUSER', 'testuser', 'on')

    def test_redis_acl_enforce_diff_commands(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='off'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on',
            commands=['GET', 'SET']
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

        redis_client_mock.execute.assert_called_with('ACL', 'SETUSER', 'testuser', 'on', 'SET', 'GET', '-@all')

    def test_redis_acl_enforce_diff_key_patterns(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='off'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on',
            key_patterns=['~dummy*']
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

        redis_client_mock.execute.assert_called_with('ACL', 'SETUSER', 'testuser', 'on', 'resetkeys', '~dummy*')

    def test_redis_acl_enforce_diff_pubsub_patterns(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='off'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on',
            pubsub_patterns=['&dummy*']
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

        redis_client_mock.execute.assert_called_with('ACL', 'SETUSER', 'testuser', 'on', 'resetchannels', '&dummy*')

    def test_redis_acl_enforce_diff_pubsub_passwords(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='off'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on',
            passwords=['dummy']
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

        redis_client_mock.execute.assert_called_with('ACL', 'SETUSER', 'testuser', 'on', 'resetpass', 'dummy')

    def test_redis_acl_enforce_diff_pubsub_passwords_nopass(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='off'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on',
            passwords=['dummy', 'nopass']
        )

        redis_client_mock = Mock()

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.operator.redis.acl_enforce(
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

        redis_client_mock.execute.assert_called_with('ACL', 'SETUSER', 'testuser', 'on', 'nopass')

    def test_redis_acl_enforce_redis_exception(self):
        redis_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='off'
        )

        spec_acl = ofredis.RedisAcl(
            logger=self.mock_logger,
            username='testuser',
            user_enable='on',
            passwords=['dummy', 'nopass']
        )

        redis_client_mock = Mock()
        redis_client_mock.execute.side_effect = [
            pyredis.exceptions.PyRedisConnClosed()
        ]

        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        self.assertRaises(
            ofredis.RedisReplicationRedisConnError,
            self.operator.redis.acl_enforce,
            pod=self.pod_secondary,
            username='testuser',
            redis_acl=redis_acl,
            spec_acl=spec_acl,
        )

    def test_redis_acls_enforce(self):
        redis_client_mock = Mock()
        self.operator.redis.connections['pod_secondary'] = redis_client_mock

        redis_acl_mock = Mock()
        self.operator.redis.acl_list = redis_client_mock
        redis_acl_mock.return_value = {
            'dummy'
        }

        spec_acls_mock = unittest.mock.patch('ofredis.RedisReplication.spec_acls', new_callable=PropertyMock)
        spec_acls_mock.return_value = {
            'user1': 'blarg'
        }

    def test_redis_acls_enforce(self):
        self.redis_acl_mock.return_value = {
            'dummy'
        }

        self.spec_acls_mock.return_value = {
            'user1': 'blarg'
        }

