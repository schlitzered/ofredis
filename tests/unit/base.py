import datetime
import os
from unittest import TestCase
from unittest.mock import Mock, patch

import pykube
import pyredis.exceptions

import ofredis


class TestRedisReplicationUnitBase(TestCase):
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

        pykube_rr_obj_patcher = patch('ofredis.PyKubeRedisReplication', autospec=True)
        self.mock_pykube_rr_obj = pykube_rr_obj_patcher.start()

        pyredis_patcher = patch('ofredis.pyredis', autospec=True)
        self.mock_pyredis = pyredis_patcher.start()
        self.mock_pyredis.exceptions = pyredis.exceptions

        self.mock_stopped = Mock()
        self.mock_spec = dict()
        self.mock_logger = Mock()
        self.mock_name = 'dummy'
        self.mock_namespace = 'default'

        self.operator = ofredis.RedisReplicationController(
            stopped=self.mock_stopped,
            spec=self.mock_spec,
            logger=self.mock_logger,
            name=self.mock_name,
            namespace=self.mock_namespace,
        )

    @staticmethod
    def create_pod_mock(
            count,
            phase='Running',
            start_time=True,
    ):
        pod = Mock()
        date = datetime.datetime.utcnow()
        date += datetime.timedelta(seconds=count)
        pod.name = f'dummy_pod{count}'
        pod.obj = {
            'status':
                {'phase': phase,
                 'conditions':
                     [],
                 'podIP': f'172.17.0.{count}',
                 }
        }
        if start_time:
            pod.obj['status']['startTime'] = date.strftime('%Y-%m-%dT%H:%M:%SZ')
        pod.labels = {}
        return pod

