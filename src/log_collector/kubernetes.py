import logging
from datetime import datetime, timezone

from src.common import LogEntry, _localize, _iso_to_dt

logger = logging.getLogger(__name__)


def _parse_log_line(line: str) -> LogEntry | None:
    line = line.rstrip("\n").rstrip("\r")
    if not line:
        return None
    if line.startswith("[") and "]" in line:
        try:
            close_bracket = line.index("]")
            ts_part = line[1:close_bracket]
            msg = line[close_bracket + 1 :].strip()
            ts = _iso_to_dt(ts_part)
            return LogEntry(timestamp=_localize(ts), stream="stdout", message=msg)
        except (ValueError, IndexError):
            pass
    first_space = line.find(" ")
    if first_space > 0:
        try:
            ts_part = line[:first_space]
            msg = line[first_space + 1 :]
            ts = _iso_to_dt(ts_part)
            return LogEntry(timestamp=_localize(ts), stream="stdout", message=msg)
        except (ValueError, IndexError):
            pass
    return LogEntry(
        timestamp=datetime.now(timezone.utc),
        stream="stdout",
        message=line,
    )


async def collect_logs(
    core_api,
    namespace: str,
    pod_name: str,
    container_name: str,
    since: datetime,
    until: datetime,
    max_bytes: int = 5242880,
) -> tuple[list[LogEntry], int]:
    if since > until:
        since, until = until, since
    since_ts = (
        since.astimezone(timezone.utc)
        if since.tzinfo is not None
        else since.replace(tzinfo=timezone.utc)
    )
    since_str = since_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    all_entries: list[LogEntry] = []
    total_size = 0

    try:
        log_text = await core_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container_name,
            timestamps=True,
            since_time=since_str,
        )

        for raw_line in log_text.splitlines():
            if total_size >= max_bytes:
                break
            entry = _parse_log_line(raw_line)
            if entry:
                all_entries.append(entry)
                total_size += len(entry.message) + 1
    except Exception as e:
        logger.error(
            "Failed to collect logs for %s/%s/%s: %s",
            namespace,
            pod_name,
            container_name,
            e,
        )

    all_entries.sort(key=lambda e: e.timestamp)
    return all_entries, total_size
