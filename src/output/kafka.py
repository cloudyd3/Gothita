import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer

from src.common import (
    event_to_kafka_dict,
    ContainerMetricsDoc,
    log_entry_to_dict,
    metric_dp_to_dict,
    _dt_to_iso,
)
from src.config import KafkaConfig

logger = logging.getLogger(__name__)


class KafkaWriter:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer: AIOKafkaProducer | None = None
        self._enabled = config.enabled

    async def connect(self):
        if not self._enabled:
            logger.info("Kafka disabled, skipping connection")
            return
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=",".join(self.config.brokers),
                client_id="container-tracker",
                acks="all",
            )
            await self.producer.start()
            await asyncio.wait_for(
                self.producer.client.fetch_all_metadata(),
                timeout=10,
            )
            self._enabled = True
            logger.info("Connected to Kafka: %s", self.config.brokers)
        except Exception as e:
            logger.warning("Kafka connection failed (%s), disabling Kafka output", e)
            if self.producer:
                await self.producer.stop()
                self.producer = None
            self._enabled = False

    async def close(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer closed")

    async def send_event(self, event):
        if not self._enabled or not self.producer:
            return
        try:
            payload = event_to_kafka_dict(event)
            payload["kafka_type"] = "event"
            await self.producer.send_and_wait(
                topic=self.config.events_topic,
                key=event.event_id.encode(),
                value=json.dumps(payload, default=str).encode(),
            )
            logger.debug(
                "Produced event %s to topic %s",
                event.event_id,
                self.config.events_topic,
            )
        except Exception as e:
            logger.warning("Failed to send event to Kafka: %s", e)

    async def send_logs(self, event):
        if not self._enabled or not self.producer or not event.logs:
            return
        try:
            payload = {
                "kafka_type": "logs",
                "event_id": event.event_id,
                "origin": {
                    "hostname": event.origin.hostname,
                    "platform": event.origin.platform,
                    "instance": event.origin.instance,
                },
                "container_name": event.name,
                "image": event.image,
                "image_id": event.image_id,
                "time_range": {
                    "started_at": _dt_to_iso(event.started_at),
                    "finished_at": _dt_to_iso(event.finished_at),
                },
                "entry_count": len(event.logs),
                "size_bytes": event.log_size,
                "logs": [log_entry_to_dict(e) for e in event.logs],
            }
            await self.producer.send_and_wait(
                topic=self.config.logs_topic,
                key=event.event_id.encode(),
                value=json.dumps(payload, default=str).encode(),
            )
            logger.debug(
                "Produced logs for event %s to topic %s",
                event.event_id,
                self.config.logs_topic,
            )
        except Exception as e:
            logger.warning("Failed to send logs to Kafka: %s", e)

    async def send_metrics(self, metrics_doc: ContainerMetricsDoc):
        if not self._enabled or not self.producer:
            return
        try:
            payload = {
                "kafka_type": "metrics",
                "event_id": metrics_doc.event_id,
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
            await self.producer.send_and_wait(
                topic=self.config.metrics_topic,
                key=(metrics_doc.event_id or metrics_doc.container_name).encode(),
                value=json.dumps(payload, default=str).encode(),
            )
            logger.debug(
                "Produced metrics for event %s to topic %s",
                metrics_doc.event_id,
                self.config.metrics_topic,
            )
        except Exception as e:
            logger.warning("Failed to send metrics to Kafka: %s", e)
