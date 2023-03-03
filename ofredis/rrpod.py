import logging

import kopf
import pykube

from ofredis.rrbase import RedisReplicationBase
from ofredis.exceptions import RedisReplicationPodError
from ofredis.exceptions import RedisReplicationPodNotReady
from ofredis.exceptions import RedisReplicationRedisConnError
from ofredis.exceptions import RedisReplicationErrorToManyPrimaries


class RedisReplicationPod(RedisReplicationBase):
    def __init__(
        self,
        log: logging.Logger,
        name: str,
        namespace: str,
        spec: dict,
        stopped,
        pod_index: dict,
    ):
        self._pod_index = pod_index
        super().__init__(
            log=log,
            name=name,
            namespace=namespace,
            spec=spec,
            stopped=stopped,
        )

    @property
    def pod_index(self):
        return self._pod_index

    @property
    def pods(self):
        pods = list()
        index_key = f"{self.namespace}_{self.name}"
        for pod in self.pod_index.get(index_key, []):
            pods.append(pod)
        return pods

    @property
    def primary(self):
        pods = self.primaries
        if len(pods) == 0:
            return None
        if len(pods) == 1:
            return pods.pop()
        self.log.error("to many primary nodes, found {0} nodes".format(len(pods)))
        raise RedisReplicationErrorToManyPrimaries

    @property
    def primary_name(self):
        pod = self.primary
        if pod:
            return pod.name
        return None

    @property
    def primaries(self):
        pods = list()
        for pod in self.pods:
            try:
                if pod.labels["RedisReplicationRole"] == "Primary":
                    pods.append(pod)
            except KeyError:
                pass
        return self._pod_remove_deleted(pods=pods)

    @property
    def secondaries(self):
        pods = list()
        for pod in self.pods:
            try:
                if pod.labels["RedisReplicationRole"] == "Secondary":
                    pods.append(pod)
            except KeyError:
                pass
        return self._pod_remove_deleted(pods=pods)

    @staticmethod
    def _pod_remove_deleted(pods):
        result = []
        for pod in pods:
            if "deletionTimestamp" not in pod.metadata:
                result.append(pod)
        return result

    def create(self):
        pod_data = {
            "apiVersion": "v1",
            "kind": "Pod",
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": self.name,
                        "image": self.spec["redis"]["image"],
                        "ports": [{"containerPort": 6379}],
                    }
                ],
            },
        }

        kopf.adopt(pod_data)
        kopf.label(pod_data, {"RedisReplication": self.name})

        pod = pykube.Pod(self.api, pod_data)
        pod.create()
        self.wait_pod_is_ready(pod=pod)

        return pod

    def delete(self, pod):
        self.log.info("{0} deleting pod".format(pod.name))
        pod.delete()
        self.wait_pod_removed_from_index(pod=pod)
        self.log.info("{0} deleting pod, done".format(pod.name))

    def _delete_candidates(self):
        candidates = []
        self.log.info("trying to find unconfigured pod")
        for pod in self.pods:
            if "RedisReplicationRole" not in pod.labels:
                candidates.append(pod)

        if candidates:
            self.log.info("found unconfigured pods")
            return candidates
        self.log.info("trying to find unconfigured pod, failed, passing secondaries")
        return self.secondaries

    def ensure_count(self):
        num_pods = len(self.pods)
        while num_pods < self.spec["replicas"]:
            self.log.info(
                "we have {0} of {1} replicas".format(num_pods, self.spec["replicas"])
            )
            self.create()
            num_pods += 1
        while num_pods > self.spec["replicas"]:
            self.log.info(
                "we have {0} of {1} replicas".format(num_pods, self.spec["replicas"])
            )
            pod = self._delete_candidates().pop()
            self.delete(pod=pod)
            num_pods -= 1

    def wait_pod_is_ready(self, pod):
        self.log.info("{0} waiting for pod to be ready".format(pod.name))
        while True:
            try:
                if self.is_ready(pod=pod):
                    self.log.info(
                        "{0} waiting for pod to be ready, done".format(pod.name)
                    )
                    return True
            except RedisReplicationPodNotReady:
                self.log.info("{0} waiting for pod to be ready".format(pod.name))
            except RedisReplicationPodError:
                self.log.info(
                    "{0} waiting for pod to be ready, failed".format(pod.name)
                )
                return
            self.wait(1)
            if self.stopped:
                return
            pod.reload()

    def wait_pod_removed_from_index(self, pod):
        self.log.info("waiting for pod to be removed from index")
        self.wait(1)
        while True:
            pod_found = False
            for _pod in self.pods:
                if _pod.name == pod.name:
                    pod_found = True
                    break
            if pod_found:
                self.log.info(f"pod {pod.name} still in index, waiting")
            else:
                self.log.info("waiting for pod to be removed from index, done")
                return
            self.wait(1)
            if self.stopped:
                return

    def get_by_name(self, pod_name):
        for pod in self.pods:
            if pod.name == pod_name:
                return pod

    def is_ready(self, pod):
        self.log.info("{0} checking if pod is running".format(pod.name))
        self.log.info(
            "{0} pod in {1} phase".format(pod.name, pod.obj["status"]["phase"])
        )
        if pod.obj["status"]["phase"] == "Running":
            self.log.info("{0} pod is ready".format(pod.name))
            return pod
        if pod.obj["status"]["phase"] == "Pending":
            raise RedisReplicationPodNotReady("{0} pod not ready".format(pod.name))
        if pod.obj["status"]["phase"] == "Failed":
            raise RedisReplicationPodError("{0} pod in error state".format(pod.name))
        if pod.obj["status"]["phase"] == "Succeeded":
            raise RedisReplicationPodError(
                "{0} pod in succeeded state".format(pod.name)
            )
        if pod.obj["status"]["phase"] == "Unknown":
            raise RedisReplicationPodError("{0} pod in unknown state".format(pod.name))
        raise RedisReplicationPodError(
            "{0} pod in and unsupported state".format(pod.name)
        )

    def set_label(self, pod, label_name, label_value, retry=3):
        self.log.info(
            "{0} setting label {1} with value {2} on pod".format(
                pod.name, label_name, label_value
            )
        )
        while retry > 0:
            try:
                pod.labels[label_name] = str(label_value)
                pod.update()
                return
            except pykube.exceptions.HTTPError as err:
                self.log.warning(
                    "{0} could not update pod: {1}, retrying".format(pod.name, err)
                )
                self.wait(1)
                retry -= 1
                pod = self.get_by_name(pod_name=pod.name)

        raise RedisReplicationRedisConnError(
            "{0} could not update pod, no more retires left".format(pod.name)
        )
