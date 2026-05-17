import logging
from datetime import datetime, timezone
import platform as py_platform

from src.common import (
    Origin,
    ContainerEvent,
    ResourceLimits,
)

logger = logging.getLogger(__name__)


async def _get_k8s_version(core_api) -> str:
    try:
        code = await core_api.get_code()
        v = code.git_version
        return v or ""
    except Exception:
        return ""


def _parse_resource_quantity(qty: str | None) -> str | None:
    return qty or None


def _extract_container_status(pod_status, container_name: str) -> dict | None:
    if not pod_status.container_statuses:
        return None
    for cs in pod_status.container_statuses:
        if cs.name == container_name:
            return cs
    return None


async def _get_node_info(core_api, node_name: str) -> tuple[str, str]:
    try:
        node = await core_api.read_node(node_name)
        labels = node.metadata.labels or {}
        return (
            labels.get("kubernetes.io/os", ""),
            labels.get("kubernetes.io/architecture", ""),
        )
    except Exception:
        return ("", "")


async def collect_container(
    core_api,
    instance_name: str,
    pod_name: str,
    container_name: str,
    namespace: str,
) -> ContainerEvent | None:
    try:
        pod = await core_api.read_namespaced_pod(pod_name, namespace)
    except Exception as e:
        logger.error("Failed to read pod %s/%s: %s", namespace, pod_name, e)
        return None

    cont_status = _extract_container_status(pod.status, container_name)
    if not cont_status:
        logger.error(
            "Container %s not found in pod %s/%s", container_name, namespace, pod_name
        )
        return None

    term_state = cont_status.state.terminated
    if term_state:
        started_dt = term_state.started_at
        if started_dt and started_dt.tzinfo is None:
            started_dt = started_dt.replace(tzinfo=timezone.utc)
        finished_dt = term_state.finished_at
        if finished_dt and finished_dt.tzinfo is None:
            finished_dt = finished_dt.replace(tzinfo=timezone.utc)
        exit_code = term_state.exit_code
        state_str = term_state.reason or "terminated"
    else:
        run_state = cont_status.state.running
        if run_state:
            started_dt = run_state.started_at
            if started_dt and started_dt.tzinfo is None:
                started_dt = started_dt.replace(tzinfo=timezone.utc)
            finished_dt = datetime.now(timezone.utc)
            exit_code = -1
            state_str = "running"
        else:
            started_dt = datetime.now(timezone.utc)
            finished_dt = datetime.now(timezone.utc)
            exit_code = (
                cont_status.last_state.terminated.exit_code
                if cont_status.last_state.terminated
                else -1
            )
            state_str = "waiting"

    pod_spec = pod.spec
    node_name = pod_spec.node_name or ""
    container_spec = None
    for c in pod_spec.containers:
        if c.name == container_name:
            container_spec = c
            break

    node_os, node_arch = await _get_node_info(core_api, node_name)

    origin = Origin(
        hostname=py_platform.node(),
        platform="kubernetes",
        instance=instance_name,
        node=node_name,
        os=node_os or py_platform.system(),
        architecture=node_arch or py_platform.machine(),
        platform_version=await _get_k8s_version(core_api),
    )

    cont_id = cont_status.container_id or ""
    if cont_id.startswith("docker://"):
        cont_id = cont_id[len("docker://") :]
    elif cont_id.startswith("containerd://"):
        cont_id = cont_id[len("containerd://") :]

    full_name = f"{namespace}/{pod.metadata.name}/{container_name}"
    image = container_spec.image if container_spec else ""

    restart_policy = None
    if pod_spec.restart_policy:
        restart_policy = {"name": pod_spec.restart_policy}

    resource_limits = None
    if container_spec and container_spec.resources:
        limits = container_spec.resources.limits or {}
        requests = container_spec.resources.requests or {}
        resource_limits = ResourceLimits(
            cpu=_parse_resource_quantity(limits.get("cpu")),
            cpu_request=_parse_resource_quantity(requests.get("cpu")),
            memory=_parse_resource_quantity(limits.get("memory")),
            memory_request=_parse_resource_quantity(requests.get("memory")),
            ephemeral_storage=_parse_resource_quantity(limits.get("ephemeral-storage")),
            ephemeral_storage_request=_parse_resource_quantity(
                requests.get("ephemeral-storage")
            ),
        )
        gpu_extras = {}
        for k, v in limits.items():
            if k not in ("cpu", "memory", "ephemeral-storage"):
                gpu_extras[k] = str(v)
        for k, v in requests.items():
            if k not in ("cpu", "memory", "ephemeral-storage", *gpu_extras.keys()):
                gpu_extras[k] = str(v)
        if gpu_extras:
            from src.common import GpuRequest

            resource_limits.gpu = [
                GpuRequest(count=int(v) if v.isdigit() else 1, capabilities=[k])
                for k, v in gpu_extras.items()
            ]

    cmd = container_spec.command if container_spec else []
    args = container_spec.args if container_spec else []
    full_cmd = cmd + args
    env = {}
    if container_spec and container_spec.env:
        for e in container_spec.env:
            if e.value:
                env[e.name] = e.value
            elif e.value_from:
                env[e.name] = f"from:{e.value_from}"

    labels = pod.metadata.labels or {}

    ports = []
    if container_spec and container_spec.ports:
        for p in container_spec.ports:
            ports.append(
                {
                    "container_port": str(p.container_port),
                    "host_port": str(p.host_port) if p.host_port else "",
                    "protocol": p.protocol or "tcp",
                }
            )

    mounts = []
    if container_spec and container_spec.volume_mounts:
        for vm in container_spec.volume_mounts:
            mounts.append(
                {
                    "source": vm.name,
                    "destination": vm.mount_path,
                    "mode": "ro" if vm.read_only else "rw",
                }
            )

    platform_specific = {
        "namespace": namespace,
        "pod_name": pod.metadata.name,
        "pod_uid": pod.metadata.uid,
        "pod_labels": pod.metadata.labels,
        "pod_annotations": pod.metadata.annotations,
        "owner_references": [
            {"kind": ref.kind, "name": ref.name, "uid": ref.uid}
            for ref in (pod.metadata.owner_references or [])
        ],
        "service_account": pod_spec.service_account_name or "",
        "host_network": pod_spec.host_network,
        "dns_policy": pod_spec.dns_policy or "",
        "priority_class_name": pod_spec.priority_class_name or "",
        "tolerations": [
            {"key": t.key, "effect": t.effect, "value": t.value, "operator": t.operator}
            for t in (pod_spec.tolerations or [])
        ],
        "node_selector": pod_spec.node_selector or {},
        "affinity": str(pod_spec.affinity) if pod_spec.affinity else None,
        "security_context": (
            str(pod_spec.security_context) if pod_spec.security_context else None
        ),
        "container_security_context": (
            str(container_spec.security_context)
            if container_spec and container_spec.security_context
            else None
        ),
        "container_restart_count": cont_status.restart_count,
        "container_id": cont_id,
        "container_image_id": cont_status.image_id or "",
        "host_ip": pod.status.host_ip or "",
        "pod_ip": pod.status.pod_ip or "",
        "qos_class": str(pod.status.qos_class or ""),
    }

    return ContainerEvent(
        event_id=f"{cont_id}_{int(finished_dt.timestamp())}",
        origin=origin,
        name=full_name,
        image=image,
        image_id=cont_status.image_id or "",
        started_at=started_dt,
        finished_at=finished_dt,
        exit_code=exit_code,
        state=state_str,
        restart_count=cont_status.restart_count,
        restart_policy=restart_policy,
        cmd=full_cmd,
        entrypoint=cmd,
        env=env,
        labels=labels,
        resource_limits=resource_limits,
        ports=ports,
        mounts=mounts,
        platform_specific=platform_specific,
    )
