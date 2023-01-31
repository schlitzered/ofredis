import logging
import kopf

import ofredis.indices
from ofredis.rrbase import RedisReplicationBase
from ofredis.rrpod import RedisReplicationPod
from ofredis.rrredis import RedisReplicationRedis
from ofredis.exceptions import RedisReplicationError
from ofredis.exceptions import RedisReplicationPodError
from ofredis.exceptions import RedisReplicationPodNotReady
from ofredis.exceptions import RedisReplicationRedisConnError
from ofredis.exceptions import RedisReplicationErrorToManyPrimaries
from ofredis.exceptions import RedisReplicationSecretMissing

logging.getLogger("urllib3").setLevel(logging.WARNING)


@kopf.on.startup()
async def configure(settings: kopf.OperatorSettings, **_):
    settings.execution.max_workers = 10000


@kopf.daemon('redis-replications')
def redis_monitor_daemon(stopped, logger, spec, name, namespace, pod_index, **__):
    redis_repl = RedisReplicationController(
        logger=logger,
        name=name,
        namespace=namespace,
        spec=spec,
        stopped=stopped,
        pod_index=pod_index
    )
    redis_repl.run()


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

    def update_object_status(self):
        self.operator_status.patch(
            {
                "status": {
                    "master": str(self.pod.primary_name),
                    "replicas": len(self.pod.pods)
                }
            },
            subresource='status'
        )

    def run(self):
        self.log.info("starting daemon name: {0} namespace: {1}".format(
            self.name, self.namespace
        ))
        self.update_object_status()
        while not self.stopped:
            self.pod.ensure_count()
            self.redis.primary_enforce()
            self.redis.cleanup()
            for pod_ns, pod_names in self.pod.pod_index.items():
                self.log.info(f"ns {pod_ns} contains {len(pod_names)}")
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
            self.stopped.wait(1)
        self.log.info("stopping daemon")
