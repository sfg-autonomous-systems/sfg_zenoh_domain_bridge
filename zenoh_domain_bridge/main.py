import argparse
import logging
import os
import time
from pathlib import Path

import zenoh

from zenoh_domain_bridge.liveliness.liveliness_bridger import LivelinessBridger

logger = logging.getLogger(__name__)
local_domain_id = int(os.getenv("ROS_DOMAIN_ID", "0"))
global_domain_id = 0


def main() -> None:
    config_filepath = os.getenv("ZENOH_SESSION_CONFIG_URI")
    config = (
        zenoh.Config.from_file(config_filepath)
        if config_filepath is not None and Path(config_filepath).exists()
        else zenoh.Config()
    )
    logger.info(f"Using Zenoh session configuration '{config_filepath}'.")
    logger.info(
        f"Bridging data from local domain ID {local_domain_id} to global domain {global_domain_id}..."
    )

    with zenoh.open(config) as session:
        _ = LivelinessBridger(
            session, node_name=f"zenoh_domain_{local_domain_id}_bridge"
        )
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
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

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
