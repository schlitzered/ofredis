import os
import unittest.mock
from unittest import TestCase
from unittest.mock import Mock, PropertyMock, patch

import pykube
import pyredis.exceptions

from tests.unit.base import TestRedisReplicationUnitBase

import ofredis


class TestRedisAclsEnforce(TestRedisReplicationUnitBase):
    def setUp(self):
        super().setUp()

        self.pod1 = pykube.Pod(
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
        redis_client_mock = Mock()
        self.operator.redis.connections['pod1'] = redis_client_mock

        self.redis_acl_mock = Mock()
        self.operator.acl_list = redis_client_mock

        self.spec_acls_mock = unittest.mock.patch('ofredis.RedisReplication.spec_acls', new_callable=PropertyMock)

    def test_redis_acls_enforce(self):
        self.redis_acl_mock.return_value = {
            'dummy'
        }

        self.spec_acls_mock.return_value = {
            'user1': 'blarg'
        }

