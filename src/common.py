import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any


def _local_tz() -> timezone:
    if time.daylight and time.localtime().tm_isdst:
        offset = -time.altzone
    else:
        offset = -time.timezone
    return timezone(timedelta(seconds=offset))


def _localize(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=_local_tz())


def _dt_to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        return dt.isoformat() + "Z"
    return dt.astimezone(timezone.utc).isoformat()


def _iso_to_dt(s: str) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


@dataclass
class Origin:
    hostname: str
    platform: str
    instance: str
    node: str | None = None
    os: str | None = None
    architecture: str | None = None
    platform_version: str | None = None


@dataclass
class GpuRequest:
    count: int = 1
    capabilities: list[str] = field(default_factory=list)
    device_ids: list[str] = field(default_factory=list)


@dataclass
class Ulimit:
    name: str
    soft: int
    hard: int


@dataclass
class OomConfig:
    disable_kill: bool = False
    score_adj: int = 0


@dataclass
class BlkioLimits:
    weight: int | None = None
    read_bps: list[dict] = field(default_factory=list)
    write_bps: list[dict] = field(default_factory=list)
    read_iops: list[dict] = field(default_factory=list)
    write_iops: list[dict] = field(default_factory=list)


@dataclass
class DeviceMapping:
    path_on_host: str
    path_in_container: str
    cgroup_permissions: str = "rwm"


@dataclass
class ResourceLimits:
    cpu: str | None = None
    cpu_request: str | None = None
    memory: str | None = None
    memory_request: str | None = None
    pid_limit: int | None = None
    ephemeral_storage: str | None = None
    ephemeral_storage_request: str | None = None
    gpu: list[GpuRequest] | None = None
    ulimits: list[Ulimit] | None = None
    oom: OomConfig | None = None
    blkio: BlkioLimits | None = None
    devices: list[DeviceMapping] | None = None


@dataclass
class LogEntry:
    timestamp: datetime
    stream: str
    message: str


@dataclass
class LogLink:
    doc_id: str | None = None
    container_name: str = ""
    time_range: dict | None = None
    entry_count: int = 0
    size_bytes: int = 0


@dataclass
class MetricDataPoint:
    timestamp: datetime
    value: float


@dataclass
class ContainerMetric:
    name: str
    unit: str
    query: str = ""
    datapoints: list[MetricDataPoint] = field(default_factory=list)


@dataclass
class MetricsLink:
    prometheus_url: str = ""
    container_name: str = ""
    query_filter: str = ""
    time_range: dict | None = None
    dedicated_doc_id: str | None = None


@dataclass
class ContainerEvent:
    event_id: str
    origin: Origin
    name: str
    image: str
    started_at: datetime
    finished_at: datetime
    exit_code: int
    state: str
    image_id: str = ""
    restart_count: int = 0
    restart_policy: dict | None = None
    cmd: list[str] = field(default_factory=list)
    entrypoint: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    resource_limits: ResourceLimits | None = None
    ports: list[dict] = field(default_factory=list)
    mounts: list[dict] = field(default_factory=list)
    volumes: list[str] = field(default_factory=list)
    logs: list[LogEntry] = field(default_factory=list)
    log_size: int = 0
    log_link: LogLink | None = None
    metrics_link: MetricsLink | None = None
    platform_specific: dict[str, Any] = field(default_factory=dict)


@dataclass
class ContainerMetricsDoc:
    event_id: str
    metrics_id: str | None = None
    origin: Origin | None = None
    container_name: str = ""
    time_range: dict | None = None
    metrics: list[ContainerMetric] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def log_entry_to_dict(entry: LogEntry) -> dict:
    return {
        "timestamp": _dt_to_iso(entry.timestamp),
        "stream": entry.stream,
        "message": entry.message,
    }


def log_entry_from_dict(data: dict) -> LogEntry:
    return LogEntry(
        timestamp=_iso_to_dt(data.get("timestamp", "")),
        stream=data.get("stream", "unknown"),
        message=data.get("message", ""),
    )


def metric_dp_to_dict(dp: MetricDataPoint) -> dict:
    return {"timestamp": _dt_to_iso(dp.timestamp), "value": dp.value}


def metric_dp_from_dict(data: dict) -> MetricDataPoint:
    return MetricDataPoint(
        timestamp=_iso_to_dt(data.get("timestamp", "")),
        value=data.get("value", 0.0),
    )


def event_to_dict(event: ContainerEvent) -> dict:
    base = asdict(event)
    base["started_at"] = _dt_to_iso(event.started_at)
    base["finished_at"] = _dt_to_iso(event.finished_at)
    base["logs"] = [log_entry_to_dict(e) for e in event.logs]
    if event.resource_limits:
        limits = asdict(event.resource_limits)
        if event.resource_limits.gpu:
            limits["gpu"] = [asdict(g) for g in event.resource_limits.gpu]
        if event.resource_limits.ulimits:
            limits["ulimits"] = [asdict(u) for u in event.resource_limits.ulimits]
        if event.resource_limits.blkio:
            limits["blkio"] = asdict(event.resource_limits.blkio)
        if event.resource_limits.devices:
            limits["devices"] = [asdict(d) for d in event.resource_limits.devices]
        if event.resource_limits.oom:
            limits["oom"] = asdict(event.resource_limits.oom)
        base["resource_limits"] = limits
    return base


def event_from_dict(data: dict) -> ContainerEvent:
    origin = Origin(**data.get("origin", {}))
    started = _iso_to_dt(data.get("started_at"))
    finished = _iso_to_dt(data.get("finished_at"))
    logs_raw = data.get("logs", [])
    logs = [log_entry_from_dict(e) for e in logs_raw]
    rl = None
    if data.get("resource_limits"):
        rl_data = data["resource_limits"]
        gpu = None
        if rl_data.get("gpu"):
            gpu = [GpuRequest(**g) for g in rl_data["gpu"]]
        ulimits = None
        if rl_data.get("ulimits"):
            ulimits = [Ulimit(**u) for u in rl_data["ulimits"]]
        oom = None
        if rl_data.get("oom"):
            oom = OomConfig(**rl_data["oom"])
        blkio = None
        if rl_data.get("blkio"):
            blkio = BlkioLimits(**rl_data["blkio"])
        devices = None
        if rl_data.get("devices"):
            devices = [DeviceMapping(**d) for d in rl_data["devices"]]
        rl = ResourceLimits(
            cpu=rl_data.get("cpu"),
            cpu_request=rl_data.get("cpu_request"),
            memory=rl_data.get("memory"),
            memory_request=rl_data.get("memory_request"),
            pid_limit=rl_data.get("pid_limit"),
            ephemeral_storage=rl_data.get("ephemeral_storage"),
            ephemeral_storage_request=rl_data.get("ephemeral_storage_request"),
            gpu=gpu,
            ulimits=ulimits,
            oom=oom,
            blkio=blkio,
            devices=devices,
        )
    log_link = None
    if data.get("log_link"):
        log_link = LogLink(**data["log_link"])
    metrics_link = None
    if data.get("metrics_link"):
        metrics_link = MetricsLink(**data["metrics_link"])
    return ContainerEvent(
        event_id=data.get("event_id", ""),
        origin=origin,
        name=data.get("name", ""),
        image=data.get("image", ""),
        started_at=started,
        finished_at=finished,
        exit_code=data.get("exit_code", -1),
        state=data.get("state", ""),
        restart_count=data.get("restart_count", 0),
        restart_policy=data.get("restart_policy"),
        cmd=data.get("cmd", []),
        entrypoint=data.get("entrypoint", []),
        env=data.get("env", {}),
        labels=data.get("labels", {}),
        resource_limits=rl,
        ports=data.get("ports", []),
        mounts=data.get("mounts", []),
        volumes=data.get("volumes", []),
        logs=logs,
        log_size=data.get("log_size", 0),
        log_link=log_link,
        metrics_link=metrics_link,
        platform_specific=data.get("platform_specific", {}),
    )


def event_to_kafka_dict(event: ContainerEvent) -> dict:
    d = event_to_dict(event)
    if event.log_link:
        d["log_link"] = asdict(event.log_link)
    if event.metrics_link:
        d["metrics_link"] = asdict(event.metrics_link)
    return d
