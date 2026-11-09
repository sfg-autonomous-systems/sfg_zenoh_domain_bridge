from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import zenoh.ext


@dataclass(frozen=True)
class QoSProfile:
    _reliability_map: ClassVar[dict[str, str]] = {
        "1": "RELIABLE",
        "2": "BEST_EFFORT",
    }
    _durability_map: ClassVar[dict[str, str]] = {
        "1": "TRANSIENT_LOCAL",
        "2": "VOLATILE",
    }
    _history_map: ClassVar[dict[str, str]] = {
        "1": "KEEP_LAST",
        "2": "KEEP_ALL",
    }

    reliability: str
    durability: str
    history: str
    depth: int

    @classmethod
    def merged_from_others(cls, profiles: list[QoSProfile]) -> QoSProfile:
        if not profiles:
            return cls(
                reliability="BEST_EFFORT",
                durability="VOLATILE",
                history="KEEP_LAST",
                depth=1,
            )

        needs_reliable = any(p.reliability == "RELIABLE" for p in profiles)
        needs_transient = any(p.durability == "TRANSIENT_LOCAL" for p in profiles)
        needs_keep_all = any(p.history == "KEEP_ALL" for p in profiles)

        return cls(
            # If any profile needs RELIABLE, the bridge must be RELIABLE.
            reliability="RELIABLE" if needs_reliable else "BEST_EFFORT",
            # If any profile is TRANSIENT_LOCAL, the bridge must be TRANSIENT_LOCAL.
            durability="TRANSIENT_LOCAL" if needs_transient else "VOLATILE",
            # If any profile needs KEEP_ALL, the bridge must use KEEP_ALL.
            history="KEEP_ALL" if needs_keep_all else "KEEP_LAST",
            # The bridge should use the largest requested depth to satisfy everyone.
            depth=max((p.depth for p in profiles), default=10),
        )

    # Based on https://github.com/ros2/rmw_zenoh/blob/kilted/rmw_zenoh_cpp/src/detail/liveliness_utils.cpp#L354-L425.
    @classmethod
    def from_key_expr(cls, key_expr: str) -> QoSProfile:
        parts = key_expr.split(":")

        if len(parts) < 6:
            raise ValueError(f"Invalid QoS string: {key_expr}")

        history_parts = parts[2].split(",")

        if len(history_parts) < 2:
            raise ValueError(f"Invalid QoS history string: {parts[2]}")

        # Determine values, falling back to defaults if the string part is empty.
        reliability = cls._reliability_map.get(parts[0], "RELIABLE")
        durability = cls._durability_map.get(parts[1], "VOLATILE")
        history = cls._history_map.get(history_parts[0], "KEEP_LAST")

        try:
            depth = int(history_parts[1]) if history_parts[1] else 10
        except ValueError:
            raise ValueError(f"Invalid QoS depth string: {history_parts[1]}")

        return cls(
            reliability=reliability,
            durability=durability,
            history=history,
            depth=depth,
        )

    # Based on https://github.com/ros2/rmw_zenoh/blob/kilted/rmw_zenoh_cpp/src/detail/rmw_publisher_data.cpp#L114-L146.
    def to_publisher_options(
        self,
    ) -> tuple[
        zenoh.CongestionControl,
        zenoh.Reliability,
        zenoh.ext.CacheConfig | None,
        zenoh.ext.MissDetectionConfig | None,
        bool,
    ]:
        congestion_control = zenoh.CongestionControl.DROP
        reliability = zenoh.Reliability.BEST_EFFORT
        cache = None
        sample_miss_detection = None
        publisher_detection = False

        if self.reliability == "RELIABLE":
            reliability = zenoh.Reliability.RELIABLE

            if self.history == "KEEP_ALL":
                congestion_control = zenoh.CongestionControl.BLOCK

        if self.durability == "TRANSIENT_LOCAL":
            publisher_detection = True
            cache = zenoh.ext.CacheConfig(max_samples=self.depth)

            if self.reliability == "RELIABLE":
                sample_miss_detection = zenoh.ext.MissDetectionConfig(
                    heartbeat=None,
                    sporadic_heartbeat=0.5,
                )

        return (
            congestion_control,
            reliability,
            cache,
            sample_miss_detection,
            publisher_detection,
        )

    # Based on https://github.com/ros2/rmw_zenoh/blob/kilted/rmw_zenoh_cpp/src/detail/rmw_subscription_data.cpp#L178-L244.
    def to_subscriber_options(
        self,
    ) -> tuple[
        zenoh.Locality | None,
        zenoh.ext.HistoryConfig | None,
        zenoh.ext.RecoveryConfig | None,
        bool,
    ]:
        # Prevents loops.
        allowed_origin = zenoh.Locality.REMOTE
        history = None
        recovery = None
        subscriber_detection = False

        if self.durability == "TRANSIENT_LOCAL":
            subscriber_detection = True
            history = zenoh.ext.HistoryConfig(
                detect_late_publishers=True,
                max_samples=self.depth,
            )

            if self.reliability == "RELIABLE":
                recovery = zenoh.ext.RecoveryConfig(
                    periodic_queries=None,
                    heartbeat=True,
                )

        return (
            allowed_origin,
            history,
            recovery,
            subscriber_detection,
        )
