import argparse
import asyncio
import logging
import sys
import os

from src.config import load_config
from src.pipeline import Pipeline

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


async def async_main(config_path: str):
    logger.info("Gothita starting")
    logger.info("Loading config from: %s", config_path)

    config = load_config(config_path)

    dkr_count = len(config.platforms["docker"].instances)
    k8s_count = len(config.platforms["kubernetes"].instances)
    logger.info(
        "Configured: %d Docker instance(s), %d K8s instance(s)", dkr_count, k8s_count
    )

    for inst in config.platforms["docker"].instances:
        logger.info(
            "  Docker: %s (socket: %s, metrics: %s)",
            inst.name,
            inst.socket,
            inst.metrics.prometheus if inst.metrics else "disabled",
        )

    for inst in config.platforms["kubernetes"].instances:
        logger.info(
            "  K8s: %s (kubeconfig: %s, metrics: %s)",
            inst.name,
            inst.kubeconfig or "in-cluster",
            inst.metrics.prometheus if inst.metrics else "disabled",
        )

    pipeline = Pipeline(config)
    await pipeline.start()


def main():
    parser = argparse.ArgumentParser(description="Gothita")
    parser.add_argument(
        "-c",
        "--config",
        default=os.environ.get("GOTHITA_CONFIG", "config.yaml"),
        help="Path to configuration file (default: config.yaml, env: GOTHITA_CONFIG)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO, env: LOG_LEVEL)",
    )
    args = parser.parse_args()

    setup_logging(args.log_level)

    try:
        asyncio.run(async_main(args.config))
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
