import logging
from dataclasses import replace

import zenoh

from .bridge_config import BridgeConfig
from .data_channel import DataChannel
from .liveliness_token import LivelinessToken

logger = logging.getLogger(__name__)


class ZenohDataBridge:
    def __init__(
        self,
        session: zenoh.Session,
        config: BridgeConfig,
    ) -> None:
        self._session = session
        self._config = config

        # We keep a reference count of the demanded and supplied data channels.
        self._demanded_data_channels: dict[DataChannel, int] = {}
        self._supplied_data_channels: dict[DataChannel, int] = {}

        # We keep track of the currently bridged data channels to avoid bridging the same channel multiple times.
        self._bridged_data_channels: dict[
            DataChannel, tuple[zenoh.Publisher, zenoh.Subscriber]
        ] = {}

    def bridge_data(self, token: LivelinessToken) -> None:
        match token.kind:
            # A supplier came online. If there is a matching demand, we need to bridge the data channel.
            case LivelinessToken.EntityKind.MESSAGE_PUBLISHER:
                supplied_data_channel = DataChannel.from_liveliness_token(token)
                self._track_demand_and_supply(
                    self._supplied_data_channels,
                    supplied_data_channel,
                    delta=1,
                )
                demanded_data_channel = replace(
                    supplied_data_channel,
                    domain_id=self._config.local_domain_id
                    if supplied_data_channel.domain_id == self._config.global_domain_id
                    else self._config.global_domain_id,
                )

                # No demand for this data channel, we don't need to bridge it.
                if demanded_data_channel not in self._demanded_data_channels:
                    return

                # There is demand, but it is already bridged.
                if supplied_data_channel in self._bridged_data_channels:
                    return

                self._bridged_data_channels[supplied_data_channel] = (
                    self._create_bridge(
                        demanded_data_channel,
                        supplied_data_channel,
                    )
                )

            # A demand came online. If there is a matching supply, we need to bridge the data channel.
            case LivelinessToken.EntityKind.MESSAGE_SUBSCRIBER:
                demanded_data_channel = DataChannel.from_liveliness_token(token)
                self._track_demand_and_supply(
                    self._demanded_data_channels,
                    demanded_data_channel,
                    delta=1,
                )
                supplied_data_channel = replace(
                    demanded_data_channel,
                    domain_id=self._config.local_domain_id
                    if demanded_data_channel.domain_id == self._config.global_domain_id
                    else self._config.global_domain_id,
                )

                # No supply for this data channel, we can't bridge it.
                if supplied_data_channel not in self._supplied_data_channels:
                    return

                # There is supply, but it is already bridged.
                if supplied_data_channel in self._bridged_data_channels:
                    return

                self._bridged_data_channels[supplied_data_channel] = (
                    self._create_bridge(
                        demanded_data_channel,
                        supplied_data_channel,
                    )
                )

    def unbridge_data(self, token: LivelinessToken) -> None:
        match token.kind:
            # A supplier went offline. If it was the only supplier for the data channel, we need to unbridge it.
            case LivelinessToken.EntityKind.MESSAGE_PUBLISHER:
                supplied_data_channel = DataChannel.from_liveliness_token(token)
                self._track_demand_and_supply(
                    self._supplied_data_channels,
                    supplied_data_channel,
                    delta=-1,
                )

                # There is still supply. We don't need to unbridge it.
                if supplied_data_channel in self._supplied_data_channels:
                    return

                if supplied_data_channel not in self._bridged_data_channels:
                    return

                publisher, subscriber = self._bridged_data_channels.pop(
                    supplied_data_channel
                )
                subscriber.undeclare()
                publisher.undeclare()

            # A demand went offline. If it was the only demand for the data channel, we need to unbridge it.
            case LivelinessToken.EntityKind.MESSAGE_SUBSCRIBER:
                demanded_data_channel = DataChannel.from_liveliness_token(token)
                self._track_demand_and_supply(
                    self._demanded_data_channels,
                    demanded_data_channel,
                    delta=-1,
                )

                supplied_data_channel = replace(
                    demanded_data_channel,
                    domain_id=self._config.local_domain_id
                    if demanded_data_channel.domain_id == self._config.global_domain_id
                    else self._config.global_domain_id,
                )

                # There is still demand. We don't need to unbridge it.
                if demanded_data_channel in self._demanded_data_channels:
                    return

                if supplied_data_channel not in self._bridged_data_channels:
                    return

                publisher, subscriber = self._bridged_data_channels.pop(
                    supplied_data_channel
                )
                subscriber.undeclare()
                publisher.undeclare()

    def _track_demand_and_supply(
        self,
        data_channels: dict[DataChannel, int],
        data_channel: DataChannel,
        delta: int,
    ) -> None:
        if data_channel not in data_channels:
            data_channels[data_channel] = 0

        data_channels[data_channel] += delta

        if data_channels[data_channel] <= 0:
            del data_channels[data_channel]

    def _create_bridge(
        self,
        demanded_data_channel: DataChannel,
        supplied_data_channel: DataChannel,
    ) -> tuple[zenoh.Publisher, zenoh.Subscriber]:
        # ToDo: Determine if we need to specify the reliability for the publisher.
        publisher = self._session.declare_publisher(demanded_data_channel.key_expr)

        def bridge(sample: zenoh.Sample):
            # Prevent loops.
            if sample.source_info is not None and str(
                sample.source_info.source_id.zid
            ) == str(self._session.zid()):
                return

            # ToDo: Perhaps we need to forward the timestamp and source info as well.
            publisher.put(
                sample.payload,
                encoding=sample.encoding,
                attachment=sample.attachment,
            )

        subscriber = self._session.declare_subscriber(
            supplied_data_channel.key_expr,
            bridge,
        )

        return publisher, subscriber
