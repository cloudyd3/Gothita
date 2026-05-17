import asyncio
import json
import logging

import aiodocker
from aiodocker.events import DockerEvents

from src.config import DockerInstanceConfig

logger = logging.getLogger(__name__)


async def watch_docker(
    config: DockerInstanceConfig, event_queue: asyncio.Queue, stop_event: asyncio.Event
):
    logger.info(
        "Docker watcher [%s] starting on %s", config.name, config.socket or config.host
    )
    while not stop_event.is_set():
        try:
            base_url = config.host or config.socket
            ssl_ctx = None
            if config.tls_verify and config.cert_path:
                import ssl

                ssl_ctx = ssl.create_default_context()
                ssl_ctx.load_cert_chain(
                    f"{config.cert_path}/cert.pem",
                    keyfile=f"{config.cert_path}/key.pem",
                )

            async with aiodocker.Docker(url=base_url, ssl_context=ssl_ctx) as docker:
                logger.info(
                    "Docker watcher [%s] connected on %s", config.name, base_url
                )

                container_starts: dict[str, object] = {}
                try:
                    raw_list = await docker.containers.list(all=True)
                    for c in raw_list:
                        info = c._container
                        cid = info.get("Id", "")
                        if cid:
                            ctn = docker.containers.container(cid)
                            try:
                                detail = await ctn.show()
                                started = detail.get("State", {}).get(
                                    "StartedAt", ""
                                )
                                if started:
                                    container_starts[cid] = started
                            except Exception:
                                pass
                except Exception as e:
                    logger.warning(
                        "Docker watcher [%s] failed to list initial containers: %s",
                        config.name,
                        e,
                    )

                filters = {
                    "type": ["container"],
                    "event": ["start", "die", "kill", "oom", "stop"],
                }
                if config.label_filters:
                    for k, v in config.label_filters.items():
                        filters.setdefault("label", []).append(f"{k}={v}")

                events = DockerEvents(docker)
                sub = events.subscribe(filters=json.dumps(filters))
                try:
                    while not stop_event.is_set():
                        try:
                            event = await asyncio.wait_for(sub.get(), timeout=1)
                        except asyncio.TimeoutError:
                            continue

                        if event is None:
                            break

                        try:
                            event_action = event.get("Action", "")
                            container_id = event.get("id") or event.get(
                                "Actor", {}
                            ).get("ID", "")
                            if not container_id:
                                continue

                            if event_action == "start":
                                container_starts[container_id] = event.get("time")
                                continue

                            if event_action == "kill":
                                exit_code = (
                                    event.get("Actor", {})
                                    .get("Attributes", {})
                                    .get("exitCode")
                                )
                                if exit_code is not None:
                                    exit_code = int(exit_code)
                                    logger.info(
                                        "Docker watcher [%s] container %s killed with code %d",
                                        config.name,
                                        container_id,
                                        exit_code,
                                    )
                                    start_time = container_starts.pop(
                                        container_id, None
                                    )
                                    await event_queue.put(
                                        {
                                            "platform": "docker",
                                            "instance": config.name,
                                            "container_id": container_id,
                                            "exit_code": exit_code,
                                            "time": event.get("time"),
                                            "start_time": start_time,
                                            "event_action": "kill",
                                        }
                                    )
                                continue

                            if event_action == "oom":
                                logger.info(
                                    "Docker watcher [%s] container %s OOM killed",
                                    config.name,
                                    container_id,
                                )
                                start_time = container_starts.pop(container_id, None)
                                await event_queue.put(
                                    {
                                        "platform": "docker",
                                        "instance": config.name,
                                        "container_id": container_id,
                                        "exit_code": 137,
                                        "time": event.get("time"),
                                        "start_time": start_time,
                                        "event_action": "oom",
                                    }
                                )
                                continue

                            if event_action in ("die", "stop"):
                                exit_code = (
                                    event.get("Actor", {})
                                    .get("Attributes", {})
                                    .get("exitCode")
                                )
                                if exit_code is not None:
                                    exit_code = int(exit_code)
                                    if exit_code != 0:
                                        logger.info(
                                            "Docker watcher [%s] container %s exited with code %d (%s)",
                                            config.name,
                                            container_id,
                                            exit_code,
                                            event_action,
                                        )
                                        start_time = container_starts.pop(
                                            container_id, None
                                        )
                                        await event_queue.put(
                                            {
                                                "platform": "docker",
                                                "instance": config.name,
                                                "container_id": container_id,
                                                "exit_code": exit_code,
                                                "time": event.get("time"),
                                                "start_time": start_time,
                                                "event_action": event_action,
                                            }
                                        )
                                continue
                        except (ValueError, KeyError) as e:
                            logger.warning(
                                "Docker watcher [%s] failed to parse event: %s",
                                config.name,
                                e,
                            )
                finally:
                    await events.stop()

        except asyncio.CancelledError:
            break
        except aiodocker.exceptions.DockerError as e:
            logger.error(
                "Docker watcher [%s] connection error: %s. Retrying in 10s...",
                config.name,
                e,
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
        except Exception as e:
            logger.error(
                "Docker watcher [%s] unexpected error: %s. Retrying in 10s...",
                config.name,
                e,
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass

    logger.info("Docker watcher [%s] stopped", config.name)
