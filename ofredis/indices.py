import kopf
import pykube


@kopf.index(
    'pods',
    labels={'RedisReplication': kopf.PRESENT}
)
def pod_index(namespace, body, **_):
    rrk_name = body['metadata']['labels'].get('RedisReplication', None)
    index_key = f'{namespace}_{rrk_name}'
    api = pykube.HTTPClient(pykube.KubeConfig.from_env())
    return {
        index_key: pykube.Pod(api, body)
    }
