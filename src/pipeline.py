import asyncio
import logging
from contextlib import asynccontextmanager

import aiodocker
import kubernetes_asyncio as k8s

from src.config import Config
from src.common import ContainerEvent, LogLink, MetricsLink, _dt_to_iso
from src.collector.docker import collect_container as docker_collect
from src.collector.kubernetes import collect_container as k8s_collect
from src.collector.metrics import collect_metrics_for_event
from src.log_collector.docker import collect_logs as docker_logs
from src.log_collector.kubernetes import collect_logs as k8s_logs
from src.watcher.docker import watch_docker
from src.watcher.kubernetes import watch_kubernetes
from src.output.mongo import MongoWriter
from src.output.kafka import KafkaWriter

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config: Config):
        self.config = config
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task] = []
        self.mongo: MongoWriter = MongoWriter(config.output["mongodb"])
        self.kafka: KafkaWriter = KafkaWriter(config.output["kafka"])

    async def start(self):
        logger.info("Starting pipeline")
        await self.mongo.connect()
        await self.kafka.connect()

        cfg = self.config

        for inst in cfg.platforms["docker"].instances:
            task = asyncio.create_task(
                watch_docker(inst, self.event_queue, self.stop_event),
                name=f"watcher-docker-{inst.name}",
            )
            self.tasks.append(task)

        for inst in cfg.platforms["kubernetes"].instances:
            task = asyncio.create_task(
                watch_kubernetes(inst, self.event_queue, self.stop_event),
                name=f"watcher-k8s-{inst.name}",
            )
            self.tasks.append(task)

        consumer = asyncio.create_task(
            self._event_consumer_loop(), name="event-consumer"
        )
        self.tasks.append(consumer)

        logger.info("Pipeline running with %d tasks", len(self.tasks))
        try:
            await self.stop_event.wait()
        except asyncio.CancelledError:
            pass
        await self.stop()

    async def stop(self):
        logger.info("Stopping pipeline")
        self.stop_event.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.mongo.close()
        await self.kafka.close()

    async def _event_consumer_loop(self):
        sem = asyncio.Semaphore(self.config.daemon.max_workers)
        while not self.stop_event.is_set():
            try:
                msg = await asyncio.wait_for(self.event_queue.get(), timeout=2)
                asyncio.create_task(self._process_event_with_sem(sem, msg))
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Event consumer error: %s", e)

    async def _process_event_with_sem(self, sem: asyncio.Semaphore, msg: dict):
        async with sem:
            await self._process_event(msg)

    async def _process_event(self, msg: dict):
        try:
            await self._do_process_event(msg)
        except Exception as e:
            logger.exception("Failed to process event %s: %s", msg, e)

    async def _do_process_event(self, msg: dict):
        platform = msg["platform"]
        instance = msg["instance"]
        logger.info("Processing event: %s/%s", platform, instance)

        event = await self._collect_event(msg)
        if not event:
            logger.warning("Failed to collect event data for %s", msg)
            return

        collected = await self._collect_logs_for_event(event, msg)
        if collected:
            event.logs, event.log_size = collected

        prom_url = self._get_platform_attr(platform, instance, "prometheus")

        log_doc_id = await self.mongo.write_logs(event)
        await self.mongo.write_event(event)
        await self.kafka.send_logs(event)
        await self.kafka.send_event(event)

        if log_doc_id:
            await self.mongo.update_event_log_link(event.event_id, log_doc_id)
            event.log_link = LogLink(
                doc_id=log_doc_id,
                container_name=event.name,
                time_range={
                    "started_at": _dt_to_iso(event.started_at),
                    "finished_at": _dt_to_iso(event.finished_at),
                },
                entry_count=len(event.logs),
                size_bytes=event.log_size,
            )

        if prom_url:
            queries = self._get_platform_attr(platform, instance, "queries")
            if queries:
                step = self._get_platform_attr(platform, instance, "step")
                metrics_doc = await collect_metrics_for_event(
                    event, prom_url, queries, step=step,
                )
                if metrics_doc:
                    metrics_id = await self.mongo.write_metrics(metrics_doc)
                    await self.mongo.update_event_metrics_link(
                        event.event_id, metrics_id
                    )
                    await self.kafka.send_metrics(metrics_doc)
                    event.metrics_link = MetricsLink(
                        prometheus_url=prom_url,
                        container_name=event.name,
                        query_filter=f"container_name={event.name}",
                        time_range={
                            "started_at": _dt_to_iso(event.started_at),
                            "finished_at": _dt_to_iso(event.finished_at),
                        },
                        dedicated_doc_id=metrics_id,
                    )

    def _get_platform_instance(self, platform_key: str, name: str):
        for inst in self.config.platforms[platform_key].instances:
            if inst.name == name:
                return inst
        return None

    @asynccontextmanager
    async def _docker_client(self, instance_name: str):
        inst = self._get_platform_instance("docker", instance_name)
        base_url = (inst.host or inst.socket) if inst else None
        ssl_ctx = None
        if inst and inst.tls_verify and inst.cert_path:
            import ssl

            ssl_ctx = ssl.create_default_context()
            ssl_ctx.load_cert_chain(
                f"{inst.cert_path}/cert.pem",
                keyfile=f"{inst.cert_path}/key.pem",
            )
        client = aiodocker.Docker(url=base_url, ssl_context=ssl_ctx)
        try:
            yield client
        finally:
            await client.close()

    @asynccontextmanager
    async def _k8s_api(self, instance_name: str):
        inst = self._get_platform_instance("kubernetes", instance_name)
        if inst and inst.kubeconfig:
            await k8s.config.load_kube_config(
                config_file=inst.kubeconfig, context=inst.context
            )
        else:
            try:
                k8s.config.load_incluster_config()
            except k8s.config.ConfigException:
                await k8s.config.load_kube_config(
                    context=inst.context if inst else None
                )
        api_client = k8s.client.ApiClient()
        try:
            yield k8s.client.CoreV1Api(api_client)
        finally:
            await api_client.close()

    async def _do_collect(self, msg: dict) -> ContainerEvent | None:
        platform = msg["platform"]
        instance = msg["instance"]

        if platform == "docker":
            async with self._docker_client(instance) as client:
                return await docker_collect(
                    client,
                    instance,
                    msg["container_id"],
                    event_exit_code=msg.get("exit_code"),
                    event_time=msg.get("time"),
                    event_start_time=msg.get("start_time"),
                    event_action=msg.get("event_action"),
                    event_restart_count=msg.get("restart_count"),
                )

        elif platform == "kubernetes":
            async with self._k8s_api(instance) as api:
                return await k8s_collect(
                    api,
                    instance,
                    msg["pod_name"],
                    msg["container_name"],
                    msg["namespace"],
                )

        return None

    async def _run_with_timeout(
        self, coro, timeout: int | None, label: str, platform: str, instance: str
    ):
        timeout = timeout or 60
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(
                "%s timed out after %ds for %s/%s",
                label,
                timeout,
                platform,
                instance,
            )
            return None

    async def _collect_event(self, msg: dict) -> ContainerEvent | None:
        return await self._run_with_timeout(
            self._do_collect(msg),
            self.config.daemon.collect_timeout,
            "Event collection",
            msg["platform"],
            msg["instance"],
        )

    async def _collect_logs_for_event(self, event, msg) -> tuple[list, int] | None:
        return await self._run_with_timeout(
            self._do_collect_logs(event, msg, self.config.daemon.max_log_bytes),
            self.config.daemon.collect_timeout,
            "Log collection",
            msg["platform"],
            msg["instance"],
        )

    async def _do_collect_logs(
        self, event, msg, max_bytes: int
    ) -> tuple[list, int] | None:
        platform = msg["platform"]
        instance = msg["instance"]

        if platform == "docker":
            async with self._docker_client(instance) as client:
                return await docker_logs(
                    client,
                    msg["container_id"],
                    event.started_at,
                    event.finished_at,
                    max_bytes,
                )

        elif platform == "kubernetes":
            async with self._k8s_api(instance) as api:
                return await k8s_logs(
                    api,
                    msg["namespace"],
                    msg["pod_name"],
                    msg["container_name"],
                    event.started_at,
                    event.finished_at,
                    max_bytes,
                )

        return None

    def _get_platform_attr(self, platform: str, instance: str, attr: str):
        for inst in self.config.platforms[platform].instances:
            if inst.name == instance and inst.metrics:
                return getattr(inst.metrics, attr, None)
        return None
