import logging

import motor.motor_asyncio

from src.common import (
    ContainerEvent,
    ContainerMetricsDoc,
    event_to_dict,
    log_entry_to_dict,
    metric_dp_to_dict,
    _dt_to_iso,
)
from src.config import MongoConfig

logger = logging.getLogger(__name__)


class MongoWriter:
    def __init__(self, config: MongoConfig):
        self.config = config
        self.client: motor.motor_asyncio.AsyncIOMotorClient | None = None
        self.db = None

    async def connect(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(self.config.uri)
        self.db = self.client[self.config.database]
        await self._ensure_indexes()
        logger.info(
            "Connected to MongoDB: %s/%s", self.config.uri, self.config.database
        )

    async def close(self):
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB connection closed")

    def _check_connected(self, raise_on_fail: bool = True):
        if self.db is not None:
            return True
        if raise_on_fail:
            raise RuntimeError("MongoDB not connected")
        return False

    async def _ensure_indexes(self):
        events = self.db[self.config.events_collection]
        await events.create_index("event_id", unique=True)
        await events.create_index("origin.platform")
        await events.create_index("origin.instance")
        await events.create_index("finished_at")
        await events.create_index("exit_code")

        logs = self.db[self.config.logs_collection]
        await logs.create_index("event_id", unique=True)
        await logs.create_index("origin.platform")

        metrics = self.db[self.config.metrics_collection]
        await metrics.create_index("event_id")
        await metrics.create_index("container_name")
        await metrics.create_index([("timestamp", 1)])

        logger.info("MongoDB indexes ensured")

    async def write_event(self, event: ContainerEvent) -> str:
        self._check_connected()
        event_doc = event_to_dict(event)
        event_doc.pop("logs", None)
        event_doc.pop("log_link", None)
        event_doc.pop("metrics_link", None)

        await self.db[self.config.events_collection].replace_one(
            {"event_id": event.event_id},
            event_doc,
            upsert=True,
        )
        logger.debug("Wrote event %s", event.event_id)
        return str(event.event_id)

    async def write_logs(self, event: ContainerEvent) -> str:
        self._check_connected()
        if not event.logs:
            return ""

        log_doc = {
            "event_id": event.event_id,
            "origin": {
                "hostname": event.origin.hostname,
                "platform": event.origin.platform,
                "instance": event.origin.instance,
                "node": event.origin.node,
            },
            "container_name": event.name,
            "time_range": {
                "started_at": _dt_to_iso(event.started_at),
                "finished_at": _dt_to_iso(event.finished_at),
            },
            "entry_count": len(event.logs),
            "size_bytes": event.log_size,
            "logs": [log_entry_to_dict(e) for e in event.logs],
        }

        result = await self.db[self.config.logs_collection].replace_one(
            {"event_id": event.event_id},
            log_doc,
            upsert=True,
        )
        logger.debug(
            "Wrote logs for event %s (%d entries)", event.event_id, len(event.logs)
        )

        inserted_id = str(result.upserted_id) if result.upserted_id else event.event_id
        return inserted_id

    async def write_metrics(self, metrics_doc: ContainerMetricsDoc) -> str:
        self._check_connected()
        doc = {
            "event_id": metrics_doc.event_id,
            "origin": {
                "hostname": metrics_doc.origin.hostname if metrics_doc.origin else "",
                "platform": metrics_doc.origin.platform if metrics_doc.origin else "",
                "instance": metrics_doc.origin.instance if metrics_doc.origin else "",
            },
            "container_name": metrics_doc.container_name,
            "time_range": metrics_doc.time_range,
            "metrics": [
                {
                    "name": m.name,
                    "unit": m.unit,
                    "query": m.query,
                    "datapoints": [metric_dp_to_dict(dp) for dp in m.datapoints],
                }
                for m in metrics_doc.metrics
            ],
            "created_at": _dt_to_iso(metrics_doc.created_at),
        }

        result = await self.db[self.config.metrics_collection].insert_one(doc)
        logger.debug(
            "Wrote metrics for event %s (%d metrics)",
            metrics_doc.event_id,
            len(metrics_doc.metrics),
        )
        return str(result.inserted_id)

    async def update_event_log_link(self, event_id: str, log_doc_id: str):
        if not self._check_connected(raise_on_fail=False):
            return
        await self.db[self.config.events_collection].update_one(
            {"event_id": event_id},
            {"$set": {"log_link.doc_id": log_doc_id}},
        )

    async def update_event_metrics_link(self, event_id: str, metrics_doc_id: str):
        if not self._check_connected(raise_on_fail=False):
            return
        await self.db[self.config.events_collection].update_one(
            {"event_id": event_id},
            {"$set": {"metrics_link.dedicated_doc_id": metrics_doc_id}},
        )
