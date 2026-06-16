import os
import re
from dataclasses import dataclass, field, fields

import yaml


def _get_default_ros_domain_id() -> int:
    try:
        return int(os.getenv("ROS_DOMAIN_ID", "0"))
    except ValueError:
        raise ValueError("ROS_DOMAIN_ID must be an integer.")


@dataclass(frozen=True)
class BridgeConfig:
    global_domain_id: int = 0
    local_domain_id: int = field(default_factory=_get_default_ros_domain_id)
    bridge_name: str | None = None
    bridged_fqns: list[str] = field(
        default_factory=lambda: [r"/global/.*", r"/tf", r"/tf_static"]
    )
    bridged_mangled_fqn_patterns: list[re.Pattern[str]] = field(init=False)

    def __post_init__(self):
        if self.bridge_name is None:
            object.__setattr__(
                self, "bridge_name", f"__zenoh_domain_{self.local_domain_id}_bridge"
            )

        bridged_fqn_patterns = [
            re.compile(raw_string.replace("/", "%")) for raw_string in self.bridged_fqns
        ]
        object.__setattr__(self, "bridged_mangled_fqn_patterns", bridged_fqn_patterns)

    @classmethod
    def from_file(cls, filepath: str) -> "BridgeConfig":
        with open(filepath, "r") as file:
            yaml_dict = yaml.safe_load(file) or {}

        valid_keys = {field.name for field in fields(cls) if field.init}
        kwargs = {key: value for key, value in yaml_dict.items() if key in valid_keys}
        return cls(**kwargs)
