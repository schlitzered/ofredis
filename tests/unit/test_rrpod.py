from unittest.mock import Mock, call, patch, PropertyMock

import pykube
import yaml

from tests.unit.base import TestRedisReplicationUnitBase

import ofredis
import ofredis.exceptions


class TestRedisReplicationPodUnit(TestRedisReplicationUnitBase):
    def setUp(self):
        super().setUp()
        self.maxDiff = None

        kopf_patcher = patch("ofredis.rrpod.kopf", autospec=True)
        self.mock_kopf = kopf_patcher.start()

        pykube_patcher = patch("ofredis.rrpod.pykube", autospec=True)
        self.mock_pykube = pykube_patcher.start()
        self.mock_pykube.exceptions = pykube.exceptions
        self.mock_pykube.HTTPClient.return_value = self.mock_pykube_instance

        self.mock_pykube_instance = Mock()
        self.mock_pykube.HTTPClient.return_value = self.mock_pykube_instance

        self.mock_pykube_pod_objects = Mock()
        self.mock_pykube.Pod.objects.return_value = self.mock_pykube_pod_objects

        self.mock_pykube_rr_obj_objects = Mock()
        self.mock_pykube_rr_obj.objects.return_value = self.mock_pykube_rr_obj_objects

        self.mock_pykube_rr_obj_instance = Mock()
        self.mock_pykube_rr_obj_objects.get_by_name.return_value = (
            self.mock_pykube_rr_obj_instance
        )

        self.mock_pykube_secret_objects = Mock()
        self.mock_pykube.Secret.objects.return_value = self.mock_pykube_secret_objects

    def test_property_pods(self):
        pod1 = self.create_pod_mock(count=1)
        pod2 = self.create_pod_mock(count=2)
        pod3 = self.create_pod_mock(count=3)
        pod4 = self.create_pod_mock(count=4)

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4,
        ]

        self.assertEqual(self.operator.pod.pods, [pod1, pod2, pod3, pod4])

    def test_property_pod_primaries(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Primary"}

        pod2 = self.create_pod_mock(2)
        pod2.labels = {"RedisReplicationRole": "Secondary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
        ]

        self.assertEqual(self.operator.pod.pod_primaries, [pod1])

    def test_property_pod_primaries_missing_label(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Primary"}

        pod2 = self.create_pod_mock(2)
        pod2.labels = {"RedisReplicationRole": "Secondary"}

        pod3 = self.create_pod_mock(2)

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
        ]

        self.assertEqual(self.operator.pod.pod_primaries, [pod1])

    def test_property_pod_primary_zero(self):
        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = []
        self.assertIsNone(self.operator.pod.pod_primary)

    def test_property_pod_primary_one(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Primary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [pod1]
        self.assertEqual(self.operator.pod.pod_primary, pod1)

    def test_property_pod_primary_many(self):
        pod1 = self.create_pod_mock(count=1)
        pod1.labels = {"RedisReplicationRole": "Primary"}
        pod2 = self.create_pod_mock(count=2)
        pod2.labels = {"RedisReplicationRole": "Primary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
        ]

        def wrap_property():
            return self.operator.pod.pod_primary

        self.assertRaises(ofredis.exceptions.RedisReplicationErrorToManyPrimaries, wrap_property)

    def test_property_pod_primary_name(self):
        pod1 = self.create_pod_mock(count=1)
        pod1.labels = {"RedisReplicationRole": "Primary"}
        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [pod1]
        self.assertEqual(self.operator.pod.pod_primary_name, "dummy_pod1")

    def test_property_pod_primary_name_none(self):
        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = []
        self.assertIsNone(self.operator.pod.pod_primary_name)

    def test_property_pod_secondaries(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Primary"}

        pod2 = self.create_pod_mock(2)
        pod2.labels = {"RedisReplicationRole": "Secondary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
        ]

        self.assertEqual(self.operator.pod.pod_secondaries, [pod2])

    def test_property_pod_secondaries_missing_label(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Primary"}

        pod2 = self.create_pod_mock(2)
        pod2.labels = {"RedisReplicationRole": "Secondary"}

        pod3 = self.create_pod_mock(2)

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
        ]

        self.assertEqual(self.operator.pod.pod_secondaries, [pod2])

    def test_property_pod_configured(self):
        pod1 = self.create_pod_mock(1)
        pod2 = self.create_pod_mock(3)
        pod2.labels["RedisReplicationRole"] = "Primary"
        pod3 = self.create_pod_mock(3)
        pod3.labels["RedisReplicationRole"] = "Secondary"
        pod4 = self.create_pod_mock(4)
        pod4.labels["RedisReplicationRole"] = "Secondary"
        pod4.metadata["deletionTimestamp"] = "2020-01-01T00:00:00Z"

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4
        ]

        candidates = self.operator.pod.pod_configured
        self.assertEqual(candidates, [pod2, pod3])

    def test_property_pod_unconfigured(self):
        pod1 = self.create_pod_mock(1)
        pod2 = self.create_pod_mock(3)
        pod2.labels["RedisReplicationRole"] = "Primary"
        pod3 = self.create_pod_mock(3)
        pod3.labels["RedisReplicationRole"] = "Secondary"
        pod4 = self.create_pod_mock(4)
        pod4.metadata["deletionTimestamp"] = "2020-01-01T00:00:00Z"

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4
        ]

        candidates = self.operator.pod.pod_unconfigured
        self.assertEqual(candidates, [pod1])

    def test_property_pod_outdated(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels["RedisReplicationRole"] = "Primary"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"
        pod2 = self.create_pod_mock(3)
        pod2.labels["RedisReplicationRole"] = "Secondary"
        pod2.labels["RedisReplicationTemplateHash"] = "1233"
        pod3 = self.create_pod_mock(3)
        pod3.labels["RedisReplicationRole"] = "Secondary"
        pod4 = self.create_pod_mock(4)
        pod4.metadata["deletionTimestamp"] = "2020-01-01T00:00:00Z"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4
        ]

        self.operator.pod._pod_template_hash = "1234"

        candidates = self.operator.pod.pod_outdated
        self.assertEqual(candidates, [pod2, pod3])

    def test_pod_create(self):
        with open("{0}/test_pod_create.yaml".format(self.data_path), "r") as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec["spec"]

        pod_data = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": self.operator.pod.name,
                        "image": self.operator.pod.spec["redis"]["image"],
                        "ports": [{"containerPort": 6379}],
                    }
                ],
            },
        }

        self.operator.pod._pod_template_hash = "1234"

        mock_create = Mock()
        self.mock_pykube.Pod.return_value = mock_create

        self.operator.pod.wait_pod_is_ready = Mock()

        self.operator.pod.create()

        self.mock_kopf.adopt.assert_called_with(pod_data)
        self.mock_kopf.label.assert_has_calls([
            call(pod_data, {"RedisReplication": self.operator.name}),
            call(pod_data, {"RedisReplicationTemplateHash": "1234"}),
        ])

        self.mock_pykube.Pod.assert_called_with(self.operator.api, pod_data)

        mock_create.create.assert_called()
        self.operator.pod.wait_pod_is_ready.assert_called_with(pod=mock_create)

    def test_pod_delete(self):
        dummy_pod = Mock()
        dummy_pod.name = "dummy_pod"
        self.operator.pod.wait_pod_removed_from_index = Mock()

        self.operator.pod.delete(dummy_pod)

        dummy_pod.delete.assert_called()
        self.operator.pod.wait_pod_removed_from_index.assert_called_with(pod=dummy_pod)

    def test_pod_delete_candidates_unconfigured(self):
        pod1 = self.create_pod_mock(1)
        pod2 = self.create_pod_mock(3)
        pod3 = self.create_pod_mock(3)
        pod3.labels["RedisReplicationRole"] = "Secondary"

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
        ]

        candidates = self.operator.pod.pod_delete_candidates
        self.assertEqual(candidates, [pod1, pod2])

    def test_pod_delete_candidates_secondaries(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Secondary"}

        pod2 = self.create_pod_mock(2)
        pod2.labels = {"RedisReplicationRole": "Secondary"}

        pod3 = self.create_pod_mock(3)
        pod3.labels = {"RedisReplicationRole": "Secondary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
        ]

        candidates = self.operator.pod.pod_delete_candidates
        self.assertEqual(candidates, [pod1, pod2, pod3])

    def test_pod_ensure_count_nothing_todo(self):
        with open("{0}/test_pod_create.yaml".format(self.data_path), "r") as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec["spec"]
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Secondary"}

        pod2 = self.create_pod_mock(2)
        pod2.labels = {"RedisReplicationRole": "Secondary"}

        pod3 = self.create_pod_mock(3)
        pod3.labels = {"RedisReplicationRole": "Secondary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
        ]

        self.operator.pod.create = Mock()
        self.operator.pod.delete = Mock()
        self.operator.pod.handle_outdated = Mock()
        self.operator.pod.operator_status_init = Mock()

        self.assertFalse(self.operator.pod.ensure_count())

        self.assertFalse(self.operator.pod.create.called)
        self.assertFalse(self.operator.pod.delete.called)
        self.assertFalse(self.operator.pod.operator_status_init.called)
        self.assertTrue(self.operator.pod.handle_outdated.called)

    def test_pod_ensure_count_two_missing(self):
        with open("{0}/test_pod_create.yaml".format(self.data_path), "r") as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec["spec"]
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Secondary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [pod1]

        self.operator.pod.create = Mock()
        self.operator.pod.wait = Mock()
        self.operator.pod.handle_outdated = Mock()

        self.operator.pod.ensure_count()

        self.assertEqual(2, self.operator.pod.create.call_count)
        self.assertTrue(self.operator.pod.handle_outdated.called)

    def test_pod_ensure_count_two_to_many(self):
        with open("{0}/test_pod_create.yaml".format(self.data_path), "r") as spec_yaml:
            spec = yaml.load(spec_yaml.read(), yaml.SafeLoader)
        self.operator.pod._spec = spec["spec"]
        pod1 = self.create_pod_mock(1)
        pod1.labels = {"RedisReplicationRole": "Secondary"}

        pod2 = self.create_pod_mock(2)
        pod2.labels = {"RedisReplicationRole": "Secondary"}

        pod3 = self.create_pod_mock(3)
        pod3.labels = {"RedisReplicationRole": "Secondary"}

        pod4 = self.create_pod_mock(4)
        pod4.labels = {"RedisReplicationRole": "Secondary"}

        pod5 = self.create_pod_mock(5)
        pod5.labels = {"RedisReplicationRole": "Secondary"}

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4,
            pod5,
        ]

        self.operator.pod.delete = Mock()
        pod_delete_candidates = type(self.operator.pod).pod_delete_candidates = PropertyMock()
        pod_delete_candidates.side_effect = [
            [pod1, pod2, pod3, pod4, pod5],
            [pod1, pod2, pod3, pod4],
        ]
        self.operator.pod.operator_status_init = Mock()
        self.operator.pod.handle_outdated = Mock()

        self.operator.pod.ensure_count()

        self.operator.pod.delete.assert_has_calls(
            [
                call(pod=pod5),
                call(pod=pod4),
            ]
        )
        self.assertEqual(2, pod_delete_candidates.call_count)
        self.assertTrue(self.operator.pod.handle_outdated.called)

    def test_handle_outdated(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels["RedisReplicationRole"] = "Primary"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"
        pod2 = self.create_pod_mock(3)
        pod2.labels["RedisReplicationRole"] = "Secondary"
        pod2.labels["RedisReplicationTemplateHash"] = "1233"
        pod3 = self.create_pod_mock(3)
        pod3.labels["RedisReplicationRole"] = "Secondary"
        pod4 = self.create_pod_mock(4)
        pod4.metadata["deletionTimestamp"] = "2020-01-01T00:00:00Z"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4
        ]

        self.operator.pod._spec = {"replicas": 3}
        self.operator.pod._pod_template_hash = "1234"
        self.operator.pod.create = Mock()
        self.operator.pod.delete = Mock()

        self.operator.pod.handle_outdated()

        self.operator.pod.delete.assert_called_once_with(pod=pod3)
        self.operator.pod.create.assert_called_once()

    def test_handle_outdated_no_outdated(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels["RedisReplicationRole"] = "Primary"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"
        pod2 = self.create_pod_mock(3)
        pod2.labels["RedisReplicationRole"] = "Secondary"
        pod2.labels["RedisReplicationTemplateHash"] = "1234"
        pod3 = self.create_pod_mock(3)
        pod3.labels["RedisReplicationRole"] = "Secondary"
        pod3.labels["RedisReplicationTemplateHash"] = "1234"
        pod4 = self.create_pod_mock(4)
        pod4.metadata["deletionTimestamp"] = "2020-01-01T00:00:00Z"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4
        ]

        self.operator.pod._spec = {"replicas": 3}
        self.operator.pod._pod_template_hash = "1234"
        self.operator.pod.create = Mock()
        self.operator.pod.delete = Mock()

        self.operator.pod.handle_outdated()

        self.operator.pod.delete.assert_not_called()
        self.operator.pod.create.assert_not_called()

    def test_handle_outdated_configured_mismatch(self):
        pod1 = self.create_pod_mock(1)
        pod1.labels["RedisReplicationRole"] = "Primary"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"
        pod2 = self.create_pod_mock(3)
        pod2.labels["RedisReplicationRole"] = "Secondary"
        pod2.labels["RedisReplicationTemplateHash"] = "1234"
        pod3 = self.create_pod_mock(3)
        pod3.labels["RedisReplicationRole"] = "Secondary"
        pod3.labels["RedisReplicationTemplateHash"] = "1234"
        pod4 = self.create_pod_mock(4)
        pod4.metadata["deletionTimestamp"] = "2020-01-01T00:00:00Z"
        pod1.labels["RedisReplicationTemplateHash"] = "1234"

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [
            pod1,
            pod2,
            pod3,
            pod4
        ]

        self.operator.pod._spec = {"replicas": 4}
        self.operator.pod._pod_template_hash = "1234"
        self.operator.pod.create = Mock()
        self.operator.pod.delete = Mock()

        self.operator.pod.handle_outdated()

        self.operator.pod.delete.assert_not_called()
        self.operator.pod.create.assert_not_called()

    def test_pod_get_by_name(self):
        pod1 = self.create_pod_mock(1)
        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [pod1]

        self.assertEqual(self.operator.pod.get_by_name(pod_name="dummy_pod1"), pod1)

    def test_pod_is_ready_running(self):
        pod1 = self.create_pod_mock(1)

        self.assertEqual(self.operator.pod.is_ready(pod=pod1), pod1)

    def test_pod_is_ready_pending(self):
        pod1 = self.create_pod_mock(1, phase="Pending")

        self.assertRaises(
            ofredis.exceptions.RedisReplicationPodNotReady, self.operator.pod.is_ready, pod=pod1
        )

    def test_pod_is_ready_failed(self):
        pod1 = self.create_pod_mock(1, phase="Failed")

        self.assertRaises(
            ofredis.exceptions.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
        )

    def test_pod_is_ready_succeeded(self):
        pod1 = self.create_pod_mock(1, phase="Succeeded")

        self.assertRaises(
            ofredis.exceptions.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
        )

    def test_pod_is_ready_unknown(self):
        pod1 = self.create_pod_mock(1, phase="Unknown")

        self.assertRaises(
            ofredis.exceptions.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
        )

    def test_pod_is_ready_real_unknown_state(self):
        pod1 = self.create_pod_mock(1, phase="WhatTheHeck")

        self.assertRaises(
            ofredis.exceptions.RedisReplicationPodError, self.operator.pod.is_ready, pod=pod1
        )

    def test_pod_set_label(self):
        pod1 = self.create_pod_mock(1)
        update_mock = Mock()
        pod1.update = update_mock

        self.operator.pod.set_label(
            pod=pod1, label_name="dummy_label", label_value="dummy_value"
        )

        update_mock.assert_called()

        self.assertEqual(pod1.labels["dummy_label"], "dummy_value")

    def test_pod_set_label_retry(self):
        pod1 = self.create_pod_mock(1)
        self.operator.pod.wait = Mock()
        update_mock = Mock()
        update_mock.side_effect = [
            pykube.exceptions.HTTPError(code=500, message="some error"),
            pykube.exceptions.HTTPError(code=500, message="some error"),
            True,
        ]
        pod1.update = update_mock

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [pod1]

        self.operator.pod.set_label(
            pod=pod1, label_name="dummy_label", label_value="dummy_value"
        )

        update_mock.assert_called()

        self.assertEqual(pod1.labels["dummy_label"], "dummy_value")
        self.operator.pod.wait.assert_called_with(1)

    def test_pod_set_label_retry_exceeded(self):
        pod1 = self.create_pod_mock(1)
        self.operator.pod.wait = Mock()
        update_mock = Mock()
        update_mock.side_effect = [
            pykube.exceptions.HTTPError(code=500, message="some error"),
            pykube.exceptions.HTTPError(code=500, message="some error"),
            pykube.exceptions.HTTPError(code=500, message="some error"),
        ]
        pod1.update = update_mock

        self.operator.pod._pod_index[f"{self.mock_namespace}_{self.mock_name}"] = [pod1]

        self.assertRaises(
            ofredis.exceptions.RedisReplicationRedisConnError,
            self.operator.pod.set_label,
            pod=pod1,
            label_name="dummy_label",
            label_value="dummy_value",
        )
        self.operator.pod.wait.assert_called_with(1)

    def test_wait_pod_is_ready(self):
        pod1 = self.create_pod_mock(1)

        is_ready = Mock()
        is_ready.side_effect = [
            ofredis.exceptions.RedisReplicationPodNotReady,
            True

        ]
        self.operator.pod.is_ready = is_ready

        wait = Mock()
        self.operator.pod.wait = wait

        self.operator.pod.wait_pod_is_ready(pod=pod1)
        is_ready.assert_has_calls([call(pod=pod1), call(pod=pod1)])
        wait.assert_called_with(1)
        pod1.reload.assert_called()

    def test_wait_pod_is_ready_pod_error(self):
        pod1 = self.create_pod_mock(1)

        is_ready = Mock()
        is_ready.side_effect = [
            ofredis.exceptions.RedisReplicationPodError

        ]
        self.operator.pod.is_ready = is_ready

        self.operator.pod.wait_pod_is_ready(pod=pod1)
        is_ready.assert_has_calls([call(pod=pod1)])

    def test_wait_pod_is_ready_stopped(self):
        pod1 = self.create_pod_mock(1)

        is_ready = Mock()
        is_ready.side_effect = [
            ofredis.exceptions.RedisReplicationPodNotReady,
            True

        ]
        self.operator.pod.is_ready = is_ready

        wait = Mock()
        self.operator.pod.wait = wait

        self.operator.pod._stopped = True

        self.operator.pod.wait_pod_is_ready(pod=pod1)
        is_ready.assert_has_calls([call(pod=pod1)])
        wait.assert_called_with(1)

    def test_wait_pod_removed_from_index(self):
        pod1 = self.create_pod_mock(1)

        pod_index = Mock()
        pod_index.get.side_effect = [
            [pod1],
            [],
        ]
        self.operator.pod._pod_index = pod_index

        wait = Mock()
        self.operator.pod.wait = wait

        self.operator.pod.wait_pod_removed_from_index(pod=pod1)

    def test_wait_pod_removed_from_index_stopped(self):
        pod1 = self.create_pod_mock(1)

        pod_index = Mock()
        pod_index.get.side_effect = [
            [pod1],
            [],
        ]
        self.operator.pod._pod_index = pod_index

        wait = Mock()
        self.operator.pod.wait = wait

        self.operator.pod._stopped = True

        self.operator.pod.wait_pod_removed_from_index(pod=pod1)

    def test_property_pod_template_hash(self):
        self.operator.pod._pod_template = {
            "key1": "value1",
            "kex2": {
                "subkey1": 1,
                "subkey2": [1, 2, "dummy"]
            }
        }

        self.assertEqual(
            self.operator.pod.pod_template_hash,
            "e2e901eb048ffb0063f31153f342d5dd7dae50a4"
        )

    def test_property_pod_template(self):
        pod_template = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "dummy",
                        "image": "dummy_image",
                        "ports": [{"containerPort": 6379}],
                    }
                ],
            },
        }

        self.operator.pod._spec = {
            "redis": {
                "image": "dummy_image",
            }
        }

        self.assertEqual(
            self.operator.pod.pod_template,
            pod_template
        )

    def test_property_pod_template_all_pod_settings(self):
        pod_template = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "annotations": {
                    "dummy": "dummy"
                },
                "labels": {
                    "dummy": "dummy"
                },
            },
            "spec": {
                "restartPolicy": "Never",
                "affinity": {
                    "podAntiAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "labelSelector": {
                                    "matchExpressions": [
                                        {
                                            "key": "app",
                                            "operator": "In",
                                            "values": ["redis"]
                                        }
                                    ]
                                },
                            }
                        ]
                    }
                },
                "containers": [
                    {
                        "name": "dummy",
                        "image": "dummy_image",
                        "ports": [{"containerPort": 6379}],
                        "resources": {
                            "limits": {
                                "cpu": "200m",
                                "memory": "256Mi"
                            },
                            "requests": {
                                "cpu": "100m",
                                "memory": "128Mi"
                            }
                        },
                        "securityContext": {
                            "runAsUser": 1000,
                            "runAsGroup": 1000,
                            "fsGroup": 1000
                        },
                    },
                    {
                        "name": "dummy",
                        "image": "dummy_image",
                    }
                ],
                "imagePullSecrets": [
                    {
                        "name": "dummy"
                    }
                ],
                "initContainers": [
                    {
                        "name": "dummy",
                        "image": "dummy_image",
                    }
                ],
                "priorityClassName": "dummy",
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsGroup": 1000,
                    "fsGroup": 1000
                },
                "serviceAccountName": "dummy",
                "nodeSelector": {
                    "dummy": "dummy"
                },
                "tolerations": [
                    {
                        "key": "dummy",
                        "operator": "Equal",
                        "value": "dummy",
                        "effect": "NoSchedule"
                    }
                ],
            }
        }

        self.operator.pod._spec = {
            "redis": {
                "image": "dummy_image",
                "resources": {
                    "limits": {
                        "cpu": "200m",
                        "memory": "256Mi"
                    },
                    "requests": {
                        "cpu": "100m",
                        "memory": "128Mi"
                    }
                },
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsGroup": 1000,
                    "fsGroup": 1000
                },
            },
            "affinity": {
                "podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "labelSelector": {
                                "matchExpressions": [
                                    {
                                        "key": "app",
                                        "operator": "In",
                                        "values": ["redis"]
                                    }
                                ]
                            },
                        }
                    ]
                }
            },
            "annotations": {
                "dummy": "dummy"
            },
            "imagePullSecrets": [
                {
                    "name": "dummy"
                }
            ],
            "initContainers": [
                {
                    "name": "dummy",
                    "image": "dummy_image",
                }
            ],
            "labels": {
                "dummy": "dummy"
            },
            "priorityClassName": "dummy",
            "securityContext": {
                "runAsUser": 1000,
                "runAsGroup": 1000,
                "fsGroup": 1000
            },
            "serviceAccountName": "dummy",
            "sideCarContainers": [
                {
                    "name": "dummy",
                    "image": "dummy_image",
                }
            ],
            "nodeSelector": {
                "dummy": "dummy"
            },
            "tolerations": [
                {
                    "key": "dummy",
                    "operator": "Equal",
                    "value": "dummy",
                    "effect": "NoSchedule"
                }
            ],
        }

        self.assertEqual(
            self.operator.pod.pod_template,
            pod_template
        )

