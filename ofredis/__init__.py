import logging
import kopf

# This is a hack to make sure the indices are loaded before the daemon starts
import ofredis.indices
from ofredis.rrcontroller import RedisReplicationController

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
    meta,
    namespace,
    pod_index,
    **__,
):  # pragma: no cover
    redis_repl = RedisReplicationController(
        logger=logger,
        meta=meta,
        name=name,
        namespace=namespace,
        spec=spec,
        stopped=stopped,
        pod_index=pod_index,
    )
    redis_repl.run()
