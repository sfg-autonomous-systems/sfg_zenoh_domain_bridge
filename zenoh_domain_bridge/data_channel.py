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

    @classmethod
    def from_key_expr(cls, key_expr: str) -> "DataChannel":
        parts = key_expr.split("/")

        # We are dealing with a data channel for a ROS 2 communication entity in the format of
        # <domain_id>/<fqn>/<type_name>/<type_hash>
        if len(parts) < 4:
            raise ValueError(f"Invalid channel key expression: {key_expr}")

        return cls(
            domain_id=int(parts[0]),
            fqn="/".join(parts[1:-2]),
            type_name=parts[-2],
            type_hash=parts[-1],
        )

    @classmethod
    def from_liveliness_token(cls, token: LivelinessToken) -> "DataChannel":
        if token.kind == LivelinessToken.EntityKind.NODE:
            raise ValueError(
                f"Cannot create channel from token with entity kind '{token.entity_kind}'."
            )

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
