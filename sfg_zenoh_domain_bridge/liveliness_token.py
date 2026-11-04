from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import ClassVar

from .qos_profile import QoSProfile


# For details on key expressions for ROS 2 liveliness tokens, see https://github.com/ros2/rmw_zenoh/blob/rolling/docs/design.md.
@dataclass(frozen=True)
class LivelinessToken:
    class EntityKind(Enum):
        NODE = "NN"
        MESSAGE_PUBLISHER = "MP"
        MESSAGE_SUBSCRIBER = "MS"
        SERVICE_SERVER = "SS"
        SERVICE_CLIENT = "SC"
        UNKNOWN = "UK"

    key_expr_prefix: ClassVar[str] = "@ros2_lv"

    domain_id: int
    session_id: str
    node_id: str
    entity_id: str
    entity_kind: str
    mangled_enclave: str
    mangled_namespace: str
    node_name: str
    mangled_fqn: str | None = None
    type_name: str | None = None
    type_hash: str | None = None
    qos: str | None = None
    backends: str | None = None

    def __repr__(self) -> str:
        parts = [
            LivelinessToken.key_expr_prefix,
            str(self.domain_id),
            f"..{self.session_id[-6:]}"
            if len(self.session_id) > 8
            else self.session_id,
            self.node_id,
            self.entity_id,
            self.entity_kind,
            self.mangled_enclave,
            self.mangled_namespace,
            self.node_name,
        ]

        if self.kind != LivelinessToken.EntityKind.NODE:
            type_hash = (
                f"..{self.type_hash[-6:]}"
                if self.type_hash and len(self.type_hash) > 8
                else self.type_hash
            )

            parts.extend(
                [
                    str(self.mangled_fqn),
                    str(self.type_name),
                    str(type_hash),
                    str(self.qos),
                ]
            )

        if self.backends is not None:
            parts.append(self.backends)

        return "/".join(parts)

    @classmethod
    def from_key_expr(cls, key_expr: str) -> "LivelinessToken":
        parts = key_expr.split("/")

        # We are dealing with a liveliness token for a ROS 2 node in the format of
        # @ros2_lv/<domain_id>/<session_id>/<node_id>/<node_id>/<entity_kind>/<mangled_enclave>/<mangled_namespace>/<node_name>
        if len(parts) == 9:
            if parts[5] != cls.EntityKind.NODE.value:
                raise ValueError(f"Invalid token key expression: {key_expr}")

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
        # @ros2_lv/<domain_id>/<session_id>/<node_id>/<entity_id>/<entity_kind>/<mangled_enclave>/<mangled_namespace>/<node_name>/<mangled_fqn>/<type_name>/<type_hash>/<qos>[/<backends>]
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
                mangled_fqn=parts[9],
                type_name=parts[10],
                type_hash=parts[11],
                qos=parts[12],
                backends=parts[13] if len(parts) > 13 else None,
            )
        else:
            raise ValueError(f"Invalid token key expression: {key_expr}")

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
            case _:
                return LivelinessToken.EntityKind.UNKNOWN

    @cached_property
    def fqn(self) -> str:
        assert self.kind != LivelinessToken.EntityKind.NODE, (
            "Node entities do not have a qualified name."
        )
        assert self.mangled_fqn is not None
        return self.mangled_fqn.replace("%", "/")

    @cached_property
    def qos_profile(self) -> QoSProfile:
        assert self.kind != LivelinessToken.EntityKind.NODE, (
            "Node entities do not have a QoS profile."
        )
        assert self.qos is not None
        return QoSProfile.from_key_expr(self.qos)

    @cached_property
    def key_expr(self) -> str:
        key_expr = f"{LivelinessToken.key_expr_prefix}/{self.domain_id}/{self.session_id}/{self.node_id}/{self.entity_id}/{self.entity_kind}/{self.mangled_enclave}/{self.mangled_namespace}/{self.node_name}"

        if self.kind != LivelinessToken.EntityKind.NODE:
            key_expr += (
                f"/{self.mangled_fqn}/{self.type_name}/{self.type_hash}/{self.qos}"
            )

        if self.backends is not None:
            key_expr += f"/{self.backends}"

        return key_expr
