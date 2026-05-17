import logging
from datetime import datetime

from src.common import LogEntry, _localize, _iso_to_dt

logger = logging.getLogger(__name__)


def _parse_docker_log_line(line: str) -> LogEntry | None:
    try:
        line = line.rstrip("\n").rstrip("\r")
        sep = line.index(" ")
        if sep < 1:
            raise ValueError(f"Invalid log line format: {line}")
        ts_part = line[:sep]
        msg = line[sep + 1 :]
        ts = _iso_to_dt(ts_part)
        return LogEntry(timestamp=_localize(ts), stream="stdout", message=msg)
    except ValueError:
        ts_part = line[:sep]
        msg = line[sep + 1 :]
        ts = datetime.fromisoformat(ts_part.replace("Z", "+00:00"))
        return LogEntry(timestamp=_localize(ts), stream="stdout", message=msg)
    except Exception as e:
        raise ValueError(f"Failed to parse log line: {line}") from e


async def collect_logs(
    docker,
    container_id: str,
    since: datetime,
    until: datetime,
    max_bytes: int = 5242880,
) -> tuple[list[LogEntry], int]:
    if since is None or until is None:
        logger.debug(
            "Skipping log collection for %s: missing time range (since=%s, until=%s)",
            container_id,
            since,
            until,
        )
        return [], 0
    if since.timestamp() > until.timestamp():
        since, until = until, since
    since_ts = int(since.timestamp())
    until_ts = int(until.timestamp())
    all_entries: list[LogEntry] = []
    total_size = 0

    for stream_name in ("stdout", "stderr"):
        try:
            container = docker.containers.container(container_id)
            logger.debug(
                "Fetching %s logs for %s since=%d until=%d",
                stream_name,
                container_id,
                since_ts,
                until_ts,
            )
            result = await container.log(
                stdout=(stream_name == "stdout"),
                stderr=(stream_name == "stderr"),
                timestamps=True,
                since=since_ts,
                until=until_ts,
            )
            if isinstance(result, bytes):
                text = result.decode("utf-8", errors="replace")
            elif isinstance(result, str):
                text = result
            elif isinstance(result, list):
                text = "".join(result)
            else:
                logger.debug(
                    "Unexpected log result type for %s (%s): %s",
                    container_id,
                    stream_name,
                    type(result).__name__,
                )
                text = ""

            if not text:
                logger.debug("No log output for %s (%s)", container_id, stream_name)
                continue
            text_bytes = text.encode("utf-8", errors="replace")
            if len(text_bytes) > max_bytes:
                text_bytes = text_bytes[:max_bytes]
                text = text_bytes.decode("utf-8", errors="replace")
            for raw_line in text.splitlines():
                if total_size >= max_bytes:
                    break
                entry = _parse_docker_log_line(raw_line)
                if entry:
                    entry_ts = entry.timestamp.timestamp()
                    if entry_ts < since_ts or entry_ts > until_ts:
                        continue
                    entry.stream = stream_name
                    all_entries.append(entry)
                    total_size += len(entry.message) + 1
        except Exception as e:
            logger.error(
                "Failed to collect logs for %s (%s): %s", container_id, stream_name, e
            )

    logger.debug(
        "Log collection for %s: %d entries, %d bytes",
        container_id,
        len(all_entries),
        total_size,
    )
    all_entries.sort(key=lambda e: e.timestamp)
    return all_entries, total_size
