import os
import yaml
from dataclasses import dataclass, field


@dataclass
class MetricsInstanceConfig:
    prometheus: str
    step: str = "15s"
    queries: dict = field(
        default_factory=lambda: {
            "cpu": 'rate(container_cpu_usage_seconds_total[30s])',
            "memory": 'container_memory_usage_bytes',
        }
    )


@dataclass
class DockerInstanceConfig:
    name: str
    socket: str = "unix:///var/run/docker.sock"
    host: str | None = None
    tls_verify: bool = False
    cert_path: str | None = None
    label_filters: dict = field(default_factory=dict)
    metrics: MetricsInstanceConfig | None = None


@dataclass
class K8sInstanceConfig:
    name: str
    kubeconfig: str | None = None
    context: str | None = None
    namespaces: list[str] = field(default_factory=list)
    label_filters: dict = field(default_factory=dict)
    metrics: MetricsInstanceConfig | None = None


@dataclass
class DockerConfig:
    instances: list[DockerInstanceConfig] = field(default_factory=list)


@dataclass
class K8sConfig:
    instances: list[K8sInstanceConfig] = field(default_factory=list)


@dataclass
class MongoConfig:
    uri: str = "mongodb://mongo:27017"
    database: str = "container_tracker"
    events_collection: str = "container_events"
    logs_collection: str = "container_logs"
    metrics_collection: str = "container_metrics"
    batch_size: int = 10

    @property
    def collections(self):
        return {
            "events": self.events_collection,
            "logs": self.logs_collection,
            "metrics": self.metrics_collection,
        }


@dataclass
class KafkaConfig:
    enabled: bool = False
    brokers: list[str] = field(default_factory=lambda: ["kafka:9092"])
    events_topic: str = "container-events"
    logs_topic: str = "container-logs"
    metrics_topic: str = "container-metrics"
    client_id: str = "gothita-tracker"
    batch_size: int = 50
    max_request_size: int = 5242880


@dataclass
class DaemonConfig:
    max_workers: int = 4
    max_log_bytes: int = 5242880
    collect_timeout: int = 60


@dataclass
class Config:
    daemon: DaemonConfig = field(default_factory=DaemonConfig)
    platforms: dict = field(
        default_factory=lambda: {"docker": DockerConfig(), "kubernetes": K8sConfig()}
    )
    output: dict = field(
        default_factory=lambda: {"mongodb": MongoConfig(), "kafka": KafkaConfig()}
    )


def _resolve_env(value):
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.environ.get(value[2:-1], "")
    if isinstance(value, str) and value == "~":
        return None
    return value


def _deep_resolve(obj):
    if isinstance(obj, dict):
        return {k: _deep_resolve(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_resolve(v) for v in obj]
    return _resolve_env(obj)


def load_config(path: str = "config.yaml") -> Config:
    with open(path) as f:
        raw = _deep_resolve(yaml.safe_load(f))

    daemon_cfg = DaemonConfig(**raw.get("daemon", {}))

    docker_instances = []
    for inst in raw.get("platforms", {}).get("docker", {}).get("instances", []):
        metrics = None
        if "metrics" in inst and inst["metrics"]:
            metrics = MetricsInstanceConfig(**inst.pop("metrics"))
        docker_instances.append(DockerInstanceConfig(**inst, metrics=metrics))

    k8s_instances = []
    for inst in raw.get("platforms", {}).get("kubernetes", {}).get("instances", []):
        metrics = None
        if "metrics" in inst and inst["metrics"]:
            metrics = MetricsInstanceConfig(**inst.pop("metrics"))
        k8s_instances.append(K8sInstanceConfig(**inst, metrics=metrics))

    mongo_cfg = MongoConfig(**raw.get("output", {}).get("mongodb", {}))
    kafka_cfg = KafkaConfig(**raw.get("output", {}).get("kafka", {}))

    return Config(
        daemon=daemon_cfg,
        platforms={
            "docker": DockerConfig(instances=docker_instances),
            "kubernetes": K8sConfig(instances=k8s_instances),
        },
        output={
            "mongodb": mongo_cfg,
            "kafka": kafka_cfg,
        },
    )
