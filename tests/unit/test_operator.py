import base64
import hashlib
import unittest.mock
from unittest.mock import Mock, PropertyMock, call
import uuid

import pykube
import pyredis.exceptions
import yaml

from tests.unit.base import TestRedisReplicationUnitBase

import ofredis


class TestRedisReplicationUnit(TestRedisReplicationUnitBase):
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

