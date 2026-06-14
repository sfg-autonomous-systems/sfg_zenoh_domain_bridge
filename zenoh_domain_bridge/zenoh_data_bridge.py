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
            DataChannel, tuple[zenoh.Publisher, zenoh.Subscriber] | zenoh.Queryable
        ] = {}

        # We keep a reference count of the demanded and supplied data channels.
        self._demanded_channels: dict[DataChannel, int] = {}
        self._supplied_channels: dict[DataChannel, int] = {}

    def bridge_data(self, token: LivelinessToken) -> None:
        self._handle_token(token, delta=1)

    def unbridge_data(self, token: LivelinessToken) -> None:
        self._handle_token(token, delta=-1)

    def _handle_token(self, token: LivelinessToken, delta: int) -> None:
        try:
            channel = DataChannel.from_liveliness_token(token)
        except ValueError as error:
            logger.error(error)
            return

        counterpart_channel = self._get_counterpart_channel(channel)

        is_publisher = token.kind in (
            LivelinessToken.EntityKind.MESSAGE_PUBLISHER,
            LivelinessToken.EntityKind.SERVICE_SERVER,
        )
        is_service = token.kind in (
            LivelinessToken.EntityKind.SERVICE_SERVER,
            LivelinessToken.EntityKind.SERVICE_CLIENT,
        )

        if is_publisher:
            supplied = channel
            demanded = counterpart_channel
            self._update_ref_count(self._supplied_channels, supplied, delta)
        else:
            supplied = counterpart_channel
            demanded = channel
            self._update_ref_count(self._demanded_channels, demanded, delta)

        if delta > 0:
            self._try_bridge(demanded, supplied, is_service)
        else:
            self._try_unbridge(supplied, demanded, is_service)

    def _try_bridge(
        self,
        demanded: DataChannel,
        supplied: DataChannel,
        is_service: bool,
    ) -> None:
        # Topics are demand and supply-driven to ensure that we only bridge channels
        # that are actually required. In contrast, services are only supply-driven because
        # (1) they do not consume resources when bridged without demand and (2) a fast lived
        # client that immediately calls the service after it appears results in zenoh dropping the
        # query because the server is not bridged in time.
        if demanded not in self._demanded_channels and not is_service:
            return

        if supplied not in self._supplied_channels:
            return

        if supplied in self._bridged_channels:
            return

        if is_service:
            self._bridged_channels[supplied] = self._create_service_bridge(
                demanded,
                supplied,
            )
        else:
            self._bridged_channels[supplied] = self._create_topic_bridge(
                demanded,
                supplied,
            )

        logger.debug(f"Bridged data channel: {supplied} -> {demanded}")

    def _try_unbridge(
        self,
        supplied: DataChannel,
        demanded: DataChannel,
        is_service: bool,
    ) -> None:
        if demanded in self._demanded_channels and not is_service:
            return

        if supplied in self._supplied_channels:
            return

        if supplied not in self._bridged_channels:
            return

        if is_service:
            queryable = self._bridged_channels.pop(supplied)
            assert isinstance(queryable, zenoh.Queryable)
            queryable.undeclare()
        else:
            publisher, subscriber = self._bridged_channels.pop(supplied)
            assert isinstance(publisher, zenoh.Publisher)
            assert isinstance(subscriber, zenoh.Subscriber)
            subscriber.undeclare()
            publisher.undeclare()

        logger.debug(f"Unbridged data channel: {supplied} -> {demanded}")

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
        ref_count = channels.get(channel, 0) + delta

        if ref_count <= 0:
            channels.pop(channel, None)
        else:
            channels[channel] = ref_count

    def _create_topic_bridge(
        self,
        demanded: DataChannel,
        supplied: DataChannel,
    ) -> tuple[zenoh.Publisher, zenoh.Subscriber]:
        # ToDo: Determine if we need to specify the reliability for the publisher.
        publisher = self._session.declare_publisher(demanded.key_expr)

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
            supplied.key_expr,
            bridge,
        )

        return publisher, subscriber

    def _create_service_bridge(
        self,
        demanded: DataChannel,
        supplied: DataChannel,
    ) -> zenoh.Queryable:
        def bridge(query: zenoh.Query):
            replies = self._session.get(
                supplied.key_expr,
                target=zenoh.QueryTarget.ALL_COMPLETE,
                payload=query.payload,
                encoding=query.encoding,
                attachment=query.attachment,
            )

            for reply in replies:
                sample = reply.result

                if isinstance(sample, zenoh.Sample):
                    query.reply(
                        query.key_expr,
                        sample.payload,
                        encoding=sample.encoding,
                        attachment=sample.attachment,
                    )
                else:
                    logger.error(f"Error from server: {sample.payload.to_string()}")
                    query.reply_err(sample.payload, encoding=sample.encoding)

        return self._session.declare_queryable(
            demanded.key_expr,
            bridge,
            complete=True,
        )
