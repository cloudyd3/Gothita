import logging
from datetime import datetime, timezone

import aiohttp

from src.common import (
    ContainerEvent,
    ContainerMetricsDoc,
    ContainerMetric,
    MetricDataPoint,
    _dt_to_iso,
)

logger = logging.getLogger(__name__)


async def _run_promql_range(
    prom_url: str, query: str, start: datetime, end: datetime, step: str = "15s"
) -> list[MetricDataPoint]:
    params = {
        "query": query,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "step": step,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{prom_url}/api/v1/query_range",
                params=params,
                timeout=aiohttp.ClientTimeout(30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        if data.get("status") != "success":
            return []
        results = data.get("data", {}).get("result", [])
        if not results:
            return []
        values = results[-1].get("values", [])
        return [
            MetricDataPoint(
                timestamp=datetime.fromtimestamp(float(v[0]), tz=timezone.utc),
                value=float(v[1]),
            )
            for v in values
        ]
    except Exception as e:
        logger.error("Prometheus range query failed: %s - %s", query, e)
        return []


async def collect_metrics_for_event(
    event: ContainerEvent, prom_url: str, queries: dict[str, str], step: str = "15s"
) -> ContainerMetricsDoc | None:
    if not prom_url:
        return None

    start = event.started_at
    end = event.finished_at
    if start.timestamp() > end.timestamp():
        start, end = end, start

    container_name = (
        event.name.split("/")[-1]
        if event.origin.platform == "kubernetes"
        else event.name
    )
    pod_name = (
        event.platform_specific.get("pod_name", "")
        if event.origin.platform == "kubernetes"
        else ""
    )
    namespace = (
        event.platform_specific.get("namespace", "")
        if event.origin.platform == "kubernetes"
        else ""
    )

    metrics_list: list[ContainerMetric] = []
    for metric_name, query_tmpl in queries.items():
        labels = {
            "Name": container_name,
            "PodName": pod_name,
            "Namespace": namespace,
        }
        query = query_tmpl
        for k, v in labels.items():
            query = query.replace(f"{{{{.{k}}}}}", str(v))

        dps = await _run_promql_range(prom_url, query, start, end, step)
        if dps:
            unit = _infer_unit(metric_name)
            metrics_list.append(
                ContainerMetric(name=metric_name, unit=unit, datapoints=dps)
            )

    if not metrics_list:
        logger.debug("No metrics found for event %s", event.event_id)
        return None

    return ContainerMetricsDoc(
        event_id=event.event_id,
        origin=event.origin,
        container_name=container_name,
        time_range={
            "started_at": _dt_to_iso(start),
            "finished_at": _dt_to_iso(end),
        },
        metrics=metrics_list,
        created_at=datetime.now(timezone.utc),
    )


def _infer_unit(metric_name: str) -> str:
    if "cpu" in metric_name:
        return "cores"
    if "memory" in metric_name or "fs" in metric_name:
        return "bytes"
    if "network" in metric_name:
        return "bytes_total"
    return "unknown"
