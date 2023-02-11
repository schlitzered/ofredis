from unittest.mock import Mock, call

from tests.unit.base import TestRedisReplicationUnitBase


class TestRedisReplicationUnit(TestRedisReplicationUnitBase):
    def test_operator_status_init(self):
        operator_status_update = Mock()
        self.operator.operator_status_update = operator_status_update

        self.operator.operator_status_init()
        operator_status_update.assert_has_calls(
            [call(key="master", value="None"), call(key="replicas", value=0)]
        )
