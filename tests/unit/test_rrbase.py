from unittest.mock import Mock

from tests.unit.base import TestRedisReplicationUnitBase


class TestRedisReplicationUnit(TestRedisReplicationUnitBase):
    def setUp(self):
        super().setUp()

        self.mock_pykube_rr_obj_objects = Mock()
        self.mock_pykube_rr_obj.objects.return_value = self.mock_pykube_rr_obj_objects

        self.mock_pykube_rr_obj_instance = Mock()
        self.mock_pykube_rr_obj_objects.get_by_name.return_value = (
            self.mock_pykube_rr_obj_instance
        )

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

    def test_property_spec(self):
        self.assertIsInstance(self.operator.spec, dict)

    def test_operator_status_update(self):
        self.operator.operator_status_update("foo", "bar")
        self.mock_pykube_rr_obj_instance.patch.assert_called_once_with(
            {"status": {"foo": "bar"}}, subresource="status"
        )

    def test_wait_timeout_1(self):
        mock_stopped = Mock()
        self.operator._stopped = mock_stopped
        self.operator.wait(timeout=1)
        mock_stopped.wait.assert_called_once_with(timeout=1)

    def test_wait_timeout_default(self):
        mock_stopped = Mock()
        self.operator._stopped = mock_stopped
        self.operator.wait()
        mock_stopped.wait.assert_called_once_with(timeout=10)
