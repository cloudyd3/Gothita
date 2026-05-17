import asyncio
import logging

import kubernetes_asyncio as k8s
import kubernetes_asyncio.watch as k8s_watch

from src.config import K8sInstanceConfig

logger = logging.getLogger(__name__)


def _detect_failed_container(pod, event_type: str) -> list[dict]:
    if event_type != "MODIFIED":
        return []

    status = pod.get("status") or {}
    cont_statuses = status.get("containerStatuses", []) or []
    failed = []

    for cs in cont_statuses:
        state = cs.get("state", {})
        terminated = state.get("terminated")
        if not terminated:
            last = cs.get("last_state", {}).get("terminated")
            if last and last.get("exit_code", 0) != 0:
                terminated = last
            else:
                continue

        exit_code = terminated.get("exit_code", 0)
        if exit_code == 0:
            continue

        metadata = pod.get("metadata", {})
        failed.append({
            "pod_name": metadata.get("name", ""),
            "container_name": cs.get("name", ""),
            "namespace": metadata.get("namespace", "default"),
            "exit_code": exit_code,
        })

    return failed


async def watch_kubernetes(config: K8sInstanceConfig, event_queue: asyncio.Queue, stop_event: asyncio.Event):
    logger.info("K8s watcher [%s] starting", config.name)
    while not stop_event.is_set():
        try:
            if config.kubeconfig:
                await k8s.config.load_kube_config(config_file=config.kubeconfig, context=config.context)
            else:
                try:
                    k8s.config.load_incluster_config()
                except k8s.config.ConfigException:
                    await k8s.config.load_kube_config(context=config.context)

            async with k8s.client.ApiClient() as api_client:
                api = k8s.client.CoreV1Api(api_client)
                w = k8s_watch.Watch()

                namespaces = config.namespaces or [None]

                for ns in namespaces:
                    if stop_event.is_set():
                        break

                    kwargs: dict = {"timeout_seconds": 0}
                    if ns:
                        kwargs["namespace"] = ns
                        logger.info("K8s watcher [%s] watching namespace %s", config.name, ns)
                    else:
                        logger.info("K8s watcher [%s] watching all namespaces", config.name)

                    async for event in w.stream(api.list_pod_for_all_namespaces, **kwargs):
                        if stop_event.is_set():
                            break

                        event_obj = event.get("object")
                        event_type = event.get("type", "")
                        if not event_obj:
                            continue

                        failed = _detect_failed_container(event_obj, event_type)
                        for f in failed:
                            logger.info("K8s watcher [%s] pod %s/%s container %s exited code %d",
                                        config.name, f["namespace"], f["pod_name"],
                                        f["container_name"], f["exit_code"])
                            await event_queue.put({
                                "platform": "kubernetes",
                                "instance": config.name,
                                "pod_name": f["pod_name"],
                                "container_name": f["container_name"],
                                "namespace": f["namespace"],
                                "exit_code": f["exit_code"],
                            })

                await w.stop()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("K8s watcher [%s] error: %s. Retrying in 10s...", config.name, e)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass

    logger.info("K8s watcher [%s] stopped", config.name)
