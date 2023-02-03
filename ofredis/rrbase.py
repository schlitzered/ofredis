import logging

import pykube


class PyKubeRedisReplication(pykube.objects.NamespacedAPIObject):
    version = 'kopf.dev/v1'
    endpoint = 'redis-replications'
    kind = 'RedisReplication'


class RedisReplicationBase:
    def __init__(
            self,
            log: logging.Logger,
            name: str,
            namespace: str,
            spec: dict,
            stopped
    ):
        self._api = None
        self._log = log
        self._name = name
        self._namespace = namespace
        self._spec = spec
        self._stopped = stopped

    @property
    def api(self):
        if not self._api:
            self._api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        return self._api

    @property
    def log(self):
        return self._log

    @property
    def name(self):
        return self._name

    @property
    def namespace(self):
        return self._namespace

    @property
    def operator_status(self):
        return PyKubeRedisReplication.objects(
            self.api,
            namespace=self.namespace
        ).get_by_name(self.name)

    @property
    def spec(self):
        return self._spec

    @property
    def stopped(self):
        return self._stopped

    def wait(self, timeout=10):
        self.stopped.wait(timeout=timeout)

    def operator_status_update(self, key, value):
        self.operator_status.patch(
            {
                "status": {
                    key: value
                }
            },
            subresource='status'
        )
