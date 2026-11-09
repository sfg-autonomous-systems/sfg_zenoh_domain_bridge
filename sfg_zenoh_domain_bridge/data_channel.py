from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property

from .liveliness_token import LivelinessToken


# For details on key expressions for ROS 2 data channels, see https://github.com/ros2/rmw_zenoh/blob/rolling/docs/design.md.
@dataclass(frozen=True)
class DataChannel:
    domain_id: int
    fqn: str
    type_name: str
    type_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "fqn", f"/{self.fqn.strip('/')}")

    def __repr__(self) -> str:
        type_hash = (
            f"..{self.type_hash[-6:]}" if len(self.type_hash) > 8 else self.type_hash
        )

        return f"{self.domain_id}/{self.fqn[1:]}/{self.type_name}/{type_hash}"

    @classmethod
    def from_liveliness_token(cls, token: LivelinessToken) -> DataChannel:
        if token.kind == LivelinessToken.EntityKind.Node:
            raise ValueError(f"Invalid entity kind: {token.entity_kind}")

        assert token.type_name is not None
        assert token.type_hash is not None

        return cls(
            domain_id=token.domain_id,
            fqn=token.fqn,
            type_name=token.type_name,
            type_hash=token.type_hash,
        )

    @cached_property
    def key_expr(self) -> str:
        return f"{self.domain_id}/{self.fqn[1:]}/{self.type_name}/{self.type_hash}"
