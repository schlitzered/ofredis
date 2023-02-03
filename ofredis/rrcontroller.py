import logging

from ofredis.rrbase import RedisReplicationBase
from ofredis.rrpod import RedisReplicationPod
from ofredis.rrredis import RedisReplicationRedis
from ofredis.exceptions import RedisReplicationPodError
from ofredis.exceptions import RedisReplicationPodNotReady
from ofredis.exceptions import RedisReplicationRedisConnError


class RedisReplicationController(RedisReplicationBase):
    def __init__(
            self,
            stopped,
            spec: dict,
            logger: logging.Logger,
            name: str,
            namespace: str,
            pod_index: dict
    ):
        super().__init__(
            log=logger,
            name=name,
            namespace=namespace,
            spec=spec,
            stopped=stopped
        )
        self._pod = RedisReplicationPod(
            log=self.log,
            name=name,
            namespace=namespace,
            spec=spec,
            stopped=stopped,
            pod_index=pod_index
        )
        self._redis = RedisReplicationRedis(
            log=self.log,
            name=name,
            namespace=namespace,
            pod=self.pod,
            spec=spec,
            stopped=stopped
        )

    @property
    def pod(self):
        return self._pod

    @property
    def redis(self):
        return self._redis

    def operator_status_init(self):
        primary_name = str(self.pod.primary_name)
        num_pods = len(self.pod.pods)
        self.log.info(f"initializing operator status with master {primary_name} and {num_pods} replicas")
        self.operator_status_update(
            key="master",
            value=primary_name
        )
        self.operator_status_update(
            key="replicas",
            value=num_pods
        )

    def run(self):  # pragma: no cover
        self.log.info("starting daemon name: {0} namespace: {1}".format(
            self.name, self.namespace
        ))
        self.operator_status_init()
        while not self.stopped:
            self.pod.ensure_count()
            self.redis.primary_enforce()
            self.redis.cleanup()
            for _pod in self.pod.pods:
                try:
                    self.redis.acls_enforce(pod=_pod)
                    self.redis.config_enforce(pod=_pod)
                    self.redis.secondary_enforce(pod=_pod)
                except RedisReplicationPodError:
                    self.pod.delete(pod=_pod)
                    self.redis.connections.pop(_pod.name, None)
                except RedisReplicationPodNotReady:
                    pass
                except RedisReplicationRedisConnError as err:
                    self.redis.connections.pop(_pod.name, None)
                    self.log.error("{0} lost connection to redis on pod: {1}".format(
                        _pod.name, err
                    ))
            self.wait(1)
        self.log.info("stopping daemon")
