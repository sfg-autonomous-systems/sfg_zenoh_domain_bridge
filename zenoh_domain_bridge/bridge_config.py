from dataclasses import dataclass


@dataclass(frozen=True)
class BridgeConfig:
    global_domain_id: int
    local_domain_id: int
    bridge_name: str
