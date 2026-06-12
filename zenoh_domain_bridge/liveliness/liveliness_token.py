import os
from dataclasses import dataclass
from enum import Enum
from functools import cached_property

local_domain_id = int(os.getenv("ROS_DOMAIN_ID", "0"))
global_domain_id = 0


# For details on the ROS 2 liveliness token format, see https://github.com/ros2/rmw_zenoh/blob/rolling/docs/design.md.
@dataclass(frozen=True)
class LivelinessToken:
    class EntityKind(Enum):
        NODE = "NN"
        MESSAGE_PUBLISHER = "MP"
        MESSAGE_SUBSCRIBER = "MS"
        SERVICE_SERVER = "SS"
        SERVICE_CLIENT = "SC"

    domain_id: int
    session_id: str
    node_id: str
    entity_id: str
    entity_kind: str
    mangled_enclave: str
    mangled_namespace: str
    node_name: str
    mangled_qualified_name: str | None = None
    type_name: str | None = None
    type_hash: str | None = None
    qos: str | None = None
    backends: str | None = None

    @cached_property
    def is_local(self) -> bool:
        return self.domain_id == local_domain_id

    @cached_property
    def is_global(self) -> bool:
        return self.domain_id == global_domain_id

    @cached_property
    def kind(self) -> EntityKind:
        match self.entity_kind:
            case "NN":
                return LivelinessToken.EntityKind.NODE
            case "MP":
                return LivelinessToken.EntityKind.MESSAGE_PUBLISHER
            case "MS":
                return LivelinessToken.EntityKind.MESSAGE_SUBSCRIBER
            case "SS":
                return LivelinessToken.EntityKind.SERVICE_SERVER
            case "SC":
                return LivelinessToken.EntityKind.SERVICE_CLIENT

    @classmethod
    def from_key(cls, key: str) -> "LivelinessToken":
        parts = key.split("/")

        # We are dealing with a liveliness token for a ROS 2 node in the format of
        # @ros2_lv/<domain_id>/<session_id>/<node_id>/<node_id>/<entity_kind>/<mangled_enclave>/<mangled_namespace>/<node_name>
        if len(parts) == 9:
            return cls(
                domain_id=int(parts[1]),
                session_id=parts[2],
                node_id=parts[3],
                entity_id=parts[4],
                entity_kind=parts[5],
                mangled_enclave=parts[6],
                mangled_namespace=parts[7],
                node_name=parts[8],
            )
        # We are dealing with a liveliness token for a ROS 2 communication entity in the format of
        # @ros2_lv/<domain_id>/<session_id>/<node_id>/<entity_id>/<entity_kind>/<mangled_enclave>/<mangled_namespace>/<node_name>/<mangled_qualified_name>/<type_name>/<type_hash>/<qos>[/<backends>]
        elif len(parts) >= 13:
            return cls(
                domain_id=int(parts[1]),
                session_id=parts[2],
                node_id=parts[3],
                entity_id=parts[4],
                entity_kind=parts[5],
                mangled_enclave=parts[6],
                mangled_namespace=parts[7],
                node_name=parts[8],
                mangled_qualified_name=parts[9],
                type_name=parts[10],
                type_hash=parts[11],
                qos=parts[12],
                backends=parts[13] if len(parts) > 13 else None,
            )
        else:
            raise ValueError(f"Invalid token key: '{key}'")

    def to_key(self) -> str:
        key = f"@ros2_lv/{self.domain_id}/{self.session_id}/{self.node_id}/{self.entity_id}/{self.entity_kind}/{self.mangled_enclave}/{self.mangled_namespace}/{self.node_name}"

        if self.kind != LivelinessToken.EntityKind.NODE:
            key += f"/{self.mangled_qualified_name}/{self.type_name}/{self.type_hash}/{self.qos}"

        if self.backends is not None:
            key += f"/{self.backends}"

        return key
