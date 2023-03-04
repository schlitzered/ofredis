import logging
import kopf

import ofredis.indices
from ofredis.rrbase import RedisReplicationBase
from ofredis.rrcontroller import RedisReplicationController
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
async def configure(settings: kopf.OperatorSettings, **_):  # pragma: no cover
    settings.execution.max_workers = 10000


@kopf.daemon("redis-replications")
def redis_monitor_daemon(
    stopped,
    logger,
    spec,
    name,
    namespace,
    pod_index,
    **__,
):  # pragma: no cover
    redis_repl = RedisReplicationController(
        logger=logger,
        name=name,
        namespace=namespace,
        spec=spec,
        stopped=stopped,
        pod_index=pod_index,
    )
    redis_repl.run()
