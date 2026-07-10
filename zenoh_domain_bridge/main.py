import argparse
import logging
import os
import time
from pathlib import Path

import zenoh

from .bridge_config import BridgeConfig
from .indented_wrapping_formatter import IndentWrappingFormatter
from .zenoh_data_bridge import ZenohDataBridge
from .zenoh_liveliness_bridge import ZenohLivelinessBridge

logger = logging.getLogger(__name__.split(".")[-1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Zenoh Domain Bridge")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        type=str.upper,
        help="Set the logging level",
    )
    args = parser.parse_args()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        IndentWrappingFormatter(
            fmt="%(asctime)s | %(name)-23s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        handlers=[console_handler],
    )

    session_config_filepath = os.getenv("ZENOH_SESSION_CONFIG_URI")
    session_config = (
        zenoh.Config.from_file(session_config_filepath)
        if session_config_filepath is not None
        and Path(session_config_filepath).exists()
        else zenoh.Config()
    )
    logger.info(f"Using session configuration: {session_config}")

    bridge_config_filepath = os.getenv("ZENOH_BRIDGE_CONFIG_URI")
    bridge_config = (
        BridgeConfig.from_file(bridge_config_filepath)
        if bridge_config_filepath is not None and Path(bridge_config_filepath).exists()
        else BridgeConfig()
    )

    if bridge_config.local_domain_id == bridge_config.global_domain_id:
        logger.critical("ROS_DOMAIN_ID must be different from the global domain ID 0.")
        exit(1)

    logger.info(f"Using bridge configuration: {bridge_config}")

    with zenoh.open(session_config) as session:
        logger.info("Waiting for Zenoh scouting to complete...")
        time.sleep(10)

        zenoh_data_bridge = ZenohDataBridge(session, bridge_config)
        _ = ZenohLivelinessBridge(
            session,
            bridge_config,
            on_token_bridged=zenoh_data_bridge.bridge_data,
            on_token_unbridged=zenoh_data_bridge.unbridge_data,
        )

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
