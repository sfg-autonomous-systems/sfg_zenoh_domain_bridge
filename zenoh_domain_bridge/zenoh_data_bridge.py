import logging
from dataclasses import replace

import zenoh

from .bridge_config import BridgeConfig
from .data_channel import DataChannel
from .liveliness_token import LivelinessToken

logger = logging.getLogger(__name__.split(".")[-1])


class ZenohDataBridge:
    def __init__(
        self,
        session: zenoh.Session,
        config: BridgeConfig,
    ) -> None:
        self._session = session
        self._config = config

        # We keep track of the currently bridged data channels to avoid bridging the same channel multiple times.
        self._bridged_channels: dict[
            DataChannel, tuple[zenoh.Publisher, zenoh.Subscriber]
        ] = {}

        # We keep a reference count of the demanded and supplied data channels.
        self._demanded_channels_ref_count: dict[DataChannel, int] = {}
        self._supplied_channels_ref_count: dict[DataChannel, int] = {}

    def bridge_data(self, token: LivelinessToken) -> None:
        channel = DataChannel.from_liveliness_token(token)

        match token.kind:
            case LivelinessToken.EntityKind.MESSAGE_PUBLISHER:
                supplied = channel
                demanded = self._get_counterpart_channel(channel)
                self._update_ref_count(
                    self._supplied_channels_ref_count, supplied, delta=1
                )
            case LivelinessToken.EntityKind.MESSAGE_SUBSCRIBER:
                demanded = channel
                supplied = self._get_counterpart_channel(channel)
                self._update_ref_count(
                    self._demanded_channels_ref_count, demanded, delta=1
                )
            case _:
                return

        self._try_bridge(demanded, supplied)

    def unbridge_data(self, token: LivelinessToken) -> None:
        channel = DataChannel.from_liveliness_token(token)

        match token.kind:
            case LivelinessToken.EntityKind.MESSAGE_PUBLISHER:
                supplied = channel
                self._update_ref_count(
                    self._supplied_channels_ref_count, supplied, delta=-1
                )
            case LivelinessToken.EntityKind.MESSAGE_SUBSCRIBER:
                demanded = channel
                supplied = self._get_counterpart_channel(channel)
                self._update_ref_count(
                    self._demanded_channels_ref_count, demanded, delta=-1
                )
            case _:
                return

        self._try_unbridge(supplied)

    def _get_counterpart_channel(self, channel: DataChannel) -> DataChannel:
        return replace(
            channel,
            domain_id=self._config.local_domain_id
            if channel.domain_id == self._config.global_domain_id
            else self._config.global_domain_id,
        )

    def _update_ref_count(
        self,
        channels: dict[DataChannel, int],
        channel: DataChannel,
        delta: int,
    ) -> None:
        new_count = channels.get(channel, 0) + delta

        if new_count <= 0:
            channels.pop(channel, None)
        else:
            channels[channel] = new_count

    def _try_bridge(self, demanded: DataChannel, supplied: DataChannel) -> None:
        if demanded not in self._demanded_channels_ref_count:
            return

        if supplied not in self._supplied_channels_ref_count:
            return

        if supplied in self._bridged_channels:
            return

        logger.debug(f"Bridging data channel: {supplied} -> {demanded}")
        self._bridged_channels[supplied] = self._create_bridge(demanded, supplied)

    def _try_unbridge(self, supplied: DataChannel) -> None:
        demanded = self._get_counterpart_channel(supplied)

        # A bridge requires BOTH supply and demand.
        has_supply = supplied in self._supplied_channels_ref_count
        has_demand = demanded in self._demanded_channels_ref_count

        if has_supply and has_demand:
            return  # The bridge is still actively required

        if supplied not in self._bridged_channels:
            return  # Already unbridged or never existed in the first place

        logger.debug(f"Unbridging data channel: {supplied} -> {demanded}")
        publisher, subscriber = self._bridged_channels.pop(supplied)
        subscriber.undeclare()
        publisher.undeclare()

    def _create_bridge(
        self,
        demanded_channel: DataChannel,
        supplied_channel: DataChannel,
    ) -> tuple[zenoh.Publisher, zenoh.Subscriber]:
        # ToDo: Determine if we need to specify the reliability for the publisher.
        publisher = self._session.declare_publisher(demanded_channel.key_expr)

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
            supplied_channel.key_expr,
            bridge,
        )

        return publisher, subscriber
