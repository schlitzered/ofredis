class RedisReplicationError(Exception):
    pass


class RedisReplicationPodError(RedisReplicationError):
    pass


class RedisReplicationRedisConnError(RedisReplicationError):
    pass


class RedisReplicationErrorToManyPrimaries(RedisReplicationError):
    pass


class RedisReplicationPodNotReady(RedisReplicationError):
    pass


class RedisReplicationSecretMissing(RedisReplicationError):
    pass
