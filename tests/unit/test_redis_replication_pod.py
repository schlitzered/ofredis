from unittest.mock import Mock, call

import pykube
import yaml

from tests.unit.base import TestRedisReplicationUnitBase

import ofredis
from ofredis import RedisReplicationErrorToManyPrimaries


class TestRedisReplicationPodUnit(TestRedisReplicationUnitBase):
    def setUp(self):
        super().setUp()

        self.mock_pykube_instance = Mock()
        self.mock_pykube.HTTPClient.return_value = self.mock_pykube_instance

        self.mock_pykube_pod_objects = Mock()
        self.mock_pykube.Pod.objects.return_value = self.mock_pykube_pod_objects

        self.mock_pykube_rr_obj_objects = Mock()
        self.mock_pykube_rr_obj.objects.return_value = self.mock_pykube_rr_obj_objects

        self.mock_pykube_rr_obj_instance = Mock()
        self.mock_pykube_rr_obj_objects.get_by_name.return_value = self.mock_pykube_rr_obj_instance

        self.mock_pykube_secret_objects = Mock()
        self.mock_pykube.Secret.objects.return_value = self.mock_pykube_secret_objects

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
            self.operator.pod.pods,
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
            self.operator.pod.primaries,
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
        self.assertIsNone(self.operator.pod.primary)

    def test_property_pod_primary_one(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        self.mock_pykube_pod_objects.filter.return_value = [pod1]
        self.assertEqual(self.operator.pod.primary, pod1)

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
            return self.operator.pod.primary

        self.assertRaises(RedisReplicationErrorToManyPrimaries, wrap_property)

    def test_property_pod_primary_name(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )
        self.mock_pykube_pod_objects.filter.return_value = [pod1]
        self.assertEqual(self.operator.pod.primary_name, 'pod1')

    def test_property_pod_primary_name_none(self):
        self.mock_pykube_pod_objects.filter.return_value = []
        self.assertIsNone(self.operator.pod.primary_name)

    def test_property_pod_secondaries(self):
        pod1 = pykube.Pod(
            api=self.mock_pykube_instance,
            obj={'metadata': {'name': 'pod1', 'namespace': 'default'}}
        )

        self.mock_pykube_pod_objects.filter.return_value = [pod1]

        self.assertEqual(
            self.operator.pod.secondaries,
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

    def test_pod_create(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec['spec']

        pod_data = {
            "apiVersion": "v1",
            "kind": "Pod",
            "spec": {
                "restartPolicy": 'Never',
                "containers": [
                    {
                        "name": self.operator.pod.name,
                        "image": self.operator.pod.spec['redis']['image'],
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

        self.operator.pod.create()

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
        self.operator.pod.delete(dummy_pod)

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

        candidates = self.operator.pod._delete_candidates()
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

        candidates = self.operator.pod._delete_candidates()
        self.assertEqual(candidates, [pod1, pod2, pod3])

    def test_pod_ensure_count_nothing_todo(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec['spec']
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

        self.operator.pod.create = Mock()
        self.operator.pod.delete = Mock()
        self.operator.pod.update_object_status = Mock()

        self.assertFalse(self.operator.pod.ensure_count())

        self.assertFalse(self.operator.pod.create.called)
        self.assertFalse(self.operator.pod.delete.called)
        self.assertFalse(self.operator.pod.update_object_status.called)

    def test_pod_ensure_count_two_missing(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec['spec']
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

        self.operator.pod.create = Mock()
        self.operator.pod.delete = Mock()

        self.operator.pod.ensure_count()

        self.assertEqual(2, self.operator.pod.create.call_count)

        self.assertFalse(self.operator.pod.delete.called)

    def test_pod_ensure_count_two_to_much(self):
        with open('{0}/test_pod_create.yaml'.format(self.data_path), 'r') as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec['spec']
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

        self.operator.pod.create = Mock()
        self.operator.pod.delete = Mock()
        self.operator.pod._delete_candidates = Mock()
        self.operator.pod._delete_candidates.side_effect = [
            [pod1, pod2, pod3, pod4, pod5],
            [pod1, pod2, pod3, pod4],
        ]
        self.operator.pod.update_object_status = Mock()

        self.operator.pod.ensure_count()

        self.operator.pod.delete.assert_has_calls([
            call(pod=pod5),
            call(pod=pod4),
        ])
        self.assertEqual(2, self.operator.pod._delete_candidates.call_count)

        self.assertFalse(self.operator.pod.create.called)

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

        self.assertEqual(self.operator.pod.get_by_name(pod_name='pod1'), pod1)

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

        self.assertEqual(self.operator.pod.is_ready(pod=pod1), pod1)

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
            ofredis.RedisReplicationPodNotReady, self.operator.pod.is_ready, pod=pod1
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
            ofredis.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
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
            ofredis.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
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
            ofredis.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
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
            ofredis.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
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

        self.operator.pod.set_label(
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

        self.operator.pod.set_label(
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
            self.operator.pod.set_label,
            pod=pod1,
            label_name='dummy_label',
            label_value='dummy_value'

        )

