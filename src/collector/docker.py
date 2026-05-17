import logging
from datetime import datetime, timezone
import platform as py_platform

from src.common import _localize, _iso_to_dt


def _parse_dt(raw: str) -> datetime:
    return _localize(_iso_to_dt(raw))


def _ensure_dt(val) -> datetime:
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val, tz=timezone.utc)
    if isinstance(val, str):
        return _parse_dt(val)
    return _localize(val)


from src.common import (
    Origin,
    ContainerEvent,
    ResourceLimits,
    GpuRequest,
    Ulimit,
    OomConfig,
    BlkioLimits,
    DeviceMapping,
)

logger = logging.getLogger(__name__)


async def _get_os_details(docker) -> tuple[str, str, str]:
    try:
        info = await docker.system.info()
        return (
            info.get("OperatingSystem", py_platform.system()),
            info.get("Architecture", py_platform.machine()),
            info.get("ServerVersion", ""),
        )
    except Exception:
        return (py_platform.system(), py_platform.machine(), "")


def _parse_env(env_list: list[str]) -> dict[str, str]:
    result = {}
    for entry in env_list:
        if "=" in entry:
            k, v = entry.split("=", 1)
            result[k] = v
    return result


def _parse_ulimits(ulimits: list[dict] | None) -> list[Ulimit] | None:
    if not ulimits:
        return None
    return [
        Ulimit(name=u.get("Name", ""), soft=u.get("Soft", 0), hard=u.get("Hard", 0))
        for u in ulimits
    ]


def _parse_gpu(devices: list[dict] | None) -> list[GpuRequest] | None:
    if not devices:
        return None
    result = []
    for d in devices:
        if (
            d.get("Driver") == "nvidia"
            or "gpu" in str(d.get("Capabilities", [])).lower()
        ):
            caps = [
                str(c[0]) if isinstance(c, list) else str(c)
                for c in d.get("Capabilities", [])
            ]
            result.append(
                GpuRequest(
                    count=d.get("Count", 1),
                    capabilities=caps,
                    device_ids=d.get("DeviceIDs", []),
                )
            )
    return result if result else None


def _parse_blkio(host_config: dict) -> BlkioLimits | None:
    weight = host_config.get("BlkioWeight")
    has_any = any(
        [
            weight,
            host_config.get("BlkioWeightDevice"),
            host_config.get("BlkioDeviceReadBps"),
            host_config.get("BlkioDeviceWriteBps"),
            host_config.get("BlkioDeviceReadIOps"),
            host_config.get("BlkioDeviceWriteIOps"),
        ]
    )
    if not has_any:
        return None
    return BlkioLimits(
        weight=weight,
        read_bps=host_config.get("BlkioDeviceReadBps", []),
        write_bps=host_config.get("BlkioDeviceWriteBps", []),
        read_iops=host_config.get("BlkioDeviceReadIOps", []),
        write_iops=host_config.get("BlkioDeviceWriteIOps", []),
    )


def _parse_devices(host_config: dict) -> list[DeviceMapping] | None:
    raw = host_config.get("Devices")
    if not raw:
        return None
    return [
        DeviceMapping(
            path_on_host=d.get("PathOnHost", ""),
            path_in_container=d.get("PathInContainer", ""),
            cgroup_permissions=d.get("CgroupPermissions", "rwm"),
        )
        for d in raw
    ]


async def collect_container(
    docker,
    instance_name: str,
    container_id: str,
    event_action: str,
    event_exit_code: int | None = None,
    event_time: int | None = None,
    event_start_time: int | None = None,
) -> ContainerEvent | None:
    try:
        container = docker.containers.container(container_id)
        inspect = await container.show()
    except Exception as e:
        logger.error("Failed to inspect container %s: %s", container_id, e)
        return None

    state = inspect.get("State", {})
    config = inspect.get("Config", {})
    host_config = inspect.get("HostConfig", {})
    name = inspect.get("Name", "").lstrip("/")
    os_info = await _get_os_details(docker)

    started_dt = _ensure_dt(event_start_time)
    finished_dt = _ensure_dt(event_time) or _parse_dt(state.get("FinishedAt", ""))

    origin = Origin(
        hostname=py_platform.node(),
        platform="docker",
        instance=instance_name,
        os=os_info[0],
        architecture=os_info[1],
        platform_version=os_info[2],
    )

    exit_code = (
        event_exit_code if event_exit_code is not None else state.get("ExitCode", -1)
    )

    action_map = {
        "die": "exited",
        "kill": "killed",
        "oom": "oom_killed",
        "stop": "stopped",
    }
    state_str = action_map.get(event_action, "exited")
    restart_count = inspect.get("RestartCount", 0)
    restart_policy_raw = host_config.get("RestartPolicy", {})
    restart_policy = None
    if restart_policy_raw and restart_policy_raw.get("Name"):
        restart_policy = {
            "name": restart_policy_raw["Name"],
            "max_retry_count": restart_policy_raw.get("MaximumRetryCount", 0),
        }

    resource_limits = ResourceLimits(
        cpu=f"{host_config.get('NanoCpus', 0)}",
        memory=host_config.get("Memory"),
        pid_limit=host_config.get("PidsLimit"),
        ulimits=_parse_ulimits(host_config.get("Ulimits")),
        gpu=_parse_gpu(host_config.get("DeviceRequests")),
        blkio=_parse_blkio(host_config),
        devices=_parse_devices(host_config),
        oom=OomConfig(
            disable_kill=host_config.get("OomKillDisable", False),
            score_adj=host_config.get("OomScoreAdj", 0),
        ),
    )

    if resource_limits.cpu == "0":
        resource_limits.cpu = None

    ports_raw = host_config.get("PortBindings", {}) or {}
    ports = []
    for container_port, bindings in ports_raw.items():
        for b in bindings:
            ports.append(
                {
                    "container_port": container_port,
                    "host_port": b.get("HostPort", ""),
                    "protocol": (
                        container_port.split("/")[1] if "/" in container_port else "tcp"
                    ),
                }
            )

    mounts_raw = inspect.get("Mounts", [])
    mounts = [
        {
            "source": m.get("Source", ""),
            "destination": m.get("Destination", ""),
            "mode": m.get("Mode", "rw"),
        }
        for m in mounts_raw
    ]

    platform_specific = {}
    for key in (
        "Healthcheck",
        "Domainname",
        "Hostname",
        "MacAddress",
        "WorkingDir",
        "User",
        "ExposedPorts",
        "StopSignal",
        "StopTimeout",
        "Shell",
        "AttachStdin",
        "AttachStdout",
        "AttachStderr",
        "OpenStdin",
        "StdinOnce",
        "Tty",
        "NetworkSettings",
        "Platform",
        "AppArmorProfile",
        "SeccompProfile",
        "CapAdd",
        "CapDrop",
        "SecurityOpt",
        "ReadonlyRootfs",
        "CgroupParent",
        "GroupAdd",
        "Init",
        "Isolation",
        "CpuShares",
        "CpuPeriod",
        "CpuQuota",
        "CpusetCpus",
        "CpusetMems",
        "KernelMemoryTCP",
        "MemoryReservation",
        "MemorySwap",
        "MemorySwappiness",
    ):
        val = inspect.get(key) or host_config.get(key) or config.get(key)
        if val is not None:
            platform_specific[key] = val

    return ContainerEvent(
        event_id=f"{container_id}_{int(finished_dt.timestamp())}",
        origin=origin,
        name=name,
        image=config.get("Image", ""),
        image_id=inspect.get("Image", ""),
        started_at=started_dt,
        finished_at=finished_dt,
        exit_code=exit_code,
        state=state_str,
        restart_count=restart_count,
        restart_policy=restart_policy,
        cmd=config.get("Cmd", []),
        entrypoint=config.get("Entrypoint", []),
        env=_parse_env(config.get("Env", [])),
        labels=config.get("Labels", {}),
        resource_limits=resource_limits,
        ports=ports,
        mounts=mounts,
        volumes=inspect.get("Volumes", []) or [],
        platform_specific=platform_specific,
    )
