from unittest.mock import Mock

import pykube

from tests.unit.base import TestRedisReplicationUnitBase

import ofredis
import ofredis.indices


class TestIndiciesUnit(TestRedisReplicationUnitBase):
    def setUp(self):
        super().setUp()

        self.mock_pykube_rr_obj_objects = Mock()
        self.mock_pykube_rr_obj.objects.return_value = self.mock_pykube_rr_obj_objects

        self.mock_pykube_rr_obj_instance = Mock()
        self.mock_pykube_rr_obj_objects.get_by_name.return_value = (
            self.mock_pykube_rr_obj_instance
        )

    def test_index_blarg(self):
        pod_obj = {"metadata": {"name": "test", "labels": {"RedisReplication": "test"}}}

        index = ofredis.indices.pod_index(namespace="blub", body=pod_obj)
        self.assertIsInstance(index["blub_test"], pykube.Pod)
