from dataclasses import dataclass
from functools import cached_property


@dataclass(frozen=True)
class BridgeConfig:
    global_domain_id: int
    local_domain_id: int
    bridge_name: str
    bridged_fqn_prefix: str = "/global/"

    @cached_property
    def mangled_bridged_fqn_prefix(self) -> str:
        return self.bridged_fqn_prefix.replace("/", "%")
