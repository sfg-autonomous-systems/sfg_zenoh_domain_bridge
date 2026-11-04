import logging
from dataclasses import replace

import zenoh
import zenoh.ext

from .bridge_config import BridgeConfig
from .data_channel import DataChannel
from .liveliness_token import LivelinessToken
from .qos_profile import QoSProfile

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
            DataChannel,
            tuple[zenoh.ext.AdvancedPublisher, zenoh.ext.AdvancedSubscriber, QoSProfile]
            | zenoh.Queryable,
        ] = {}

        # We keep a track of the demanded and supplied data channels and their corresponding tokens.
        self._demanded_channels: dict[DataChannel, set[LivelinessToken]] = {}
        self._supplied_channels: dict[DataChannel, set[LivelinessToken]] = {}

    def bridge_data(self, token: LivelinessToken) -> None:
        self._handle_token(token, add=True)

    def unbridge_data(self, token: LivelinessToken) -> None:
        self._handle_token(token, add=False)

    def _handle_token(self, token: LivelinessToken, add: bool) -> None:
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
            self._update_channels(self._supplied_channels, supplied, token, add)
        else:
            supplied = counterpart_channel
            demanded = channel
            self._update_channels(self._demanded_channels, demanded, token, add)

        self._evaluate_bridge_state(demanded, supplied, is_service)

    def _evaluate_bridge_state(
        self,
        demanded: DataChannel,
        supplied: DataChannel,
        is_service: bool,
    ) -> None:
        has_demand = demanded in self._demanded_channels
        has_supply = supplied in self._supplied_channels

        # Topics are demand and supply-driven to ensure that we only bridge channels
        # that are actually required. In contrast, services are only supply-driven because
        # (1) they do not consume resources when bridged without demand and (2) a fast lived
        # client that immediately calls the service after it appears results in Zenoh dropping the
        # query because the server is not bridged in time.
        should_be_bridged = has_supply if is_service else (has_demand and has_supply)

        if not should_be_bridged:
            self._unbridge(demanded, supplied, is_service)
            return

        if is_service:
            if supplied not in self._bridged_channels:
                self._bridged_channels[supplied] = self._bridge_service(
                    demanded,
                    supplied,
                )
        else:
            demanded_profiles = [
                token.qos_profile
                for token in self._demanded_channels.get(demanded, set())
            ]
            supplied_profiles = [
                token.qos_profile
                for token in self._supplied_channels.get(supplied, set())
            ]
            merged_qos = QoSProfile.merged_from_others(
                demanded_profiles + supplied_profiles
            )

            if supplied in self._bridged_channels:
                _, _, existing_qos = self._bridged_channels[supplied]
                assert isinstance(existing_qos, QoSProfile)

                if existing_qos == merged_qos:
                    return

                logger.debug(
                    f"Rebuilding bridge for {supplied} due to QoS change: {existing_qos.reliability} -> {merged_qos.reliability}"
                )
                self._unbridge(demanded, supplied, is_service)

            self._bridged_channels[supplied] = self._bridge_topic(
                demanded,
                supplied,
                merged_qos,
            )

        logger.debug(f"Bridged data channel: {supplied} -> {demanded}")

    def _bridge_topic(
        self,
        demanded: DataChannel,
        supplied: DataChannel,
        qos: QoSProfile,
    ) -> tuple[zenoh.ext.AdvancedPublisher, zenoh.ext.AdvancedSubscriber, QoSProfile]:
        publisher_options = qos.to_publisher_options()
        publisher = zenoh.ext.declare_advanced_publisher(
            self._session,
            demanded.key_expr,
            congestion_control=publisher_options[0],
            reliability=publisher_options[1],
            cache=publisher_options[2],
            sample_miss_detection=publisher_options[3],
            publisher_detection=publisher_options[4],
        )

        def bridge(sample: zenoh.Sample):
            publisher.put(
                sample.payload,
                encoding=sample.encoding,
                attachment=sample.attachment,
            )

        subscriber_options = qos.to_subscriber_options()
        subscriber = zenoh.ext.declare_advanced_subscriber(
            self._session,
            supplied.key_expr,
            bridge,
            allowed_origin=subscriber_options[0],
            history=subscriber_options[1],
            recovery=subscriber_options[2],
            subscriber_detection=subscriber_options[3],
        )
        return publisher, subscriber, qos

    def _bridge_service(
        self,
        demanded: DataChannel,
        supplied: DataChannel,
    ) -> zenoh.Queryable:
        def bridge(query: zenoh.Query):
            def forward(reply: zenoh.Reply) -> None:
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

            timeout = None
            # Zenoh ends a query after 10 s by default. The get_result service of an action
            # only replies once the goal has finished, which can take much longer, so the bridge
            # must not end that query early. rmw_zenoh makes the same exception for its clients:
            # https://github.com/ros2/rmw_zenoh/blob/c49176dc15edfcb1f176f7bfb14cf71469cfd341/rmw_zenoh_cpp/src/detail/rmw_client_data.cpp#L137-L146
            if supplied.fqn.endswith("/_action/get_result"):
                timeout = 100 * 365 * 24 * 3600

            self._session.get(
                supplied.key_expr,
                target=zenoh.QueryTarget.BEST_MATCHING,
                payload=query.payload,
                encoding=query.encoding,
                attachment=query.attachment,
                handler=zenoh.handlers.Callback(forward, query.drop),
                timeout=timeout,
            )

        return self._session.declare_queryable(
            demanded.key_expr,
            bridge,
            complete=True,
        )

    def _unbridge(
        self,
        demanded: DataChannel,
        supplied: DataChannel,
        is_service: bool,
    ) -> None:
        if supplied not in self._bridged_channels:
            return

        if is_service:
            queryable = self._bridged_channels.pop(supplied)
            assert isinstance(queryable, zenoh.Queryable)
            queryable.undeclare()
        else:
            publisher, subscriber, _ = self._bridged_channels.pop(supplied)
            assert isinstance(publisher, zenoh.ext.AdvancedPublisher)
            assert isinstance(subscriber, zenoh.ext.AdvancedSubscriber)
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

    def _update_channels(
        self,
        channels: dict[DataChannel, set[LivelinessToken]],
        channel: DataChannel,
        token: LivelinessToken,
        add: bool,
    ) -> None:
        if add:
            channels.setdefault(channel, set()).add(token)
        elif channel in channels:
            channels[channel].discard(token)

            if not channels[channel]:
                del channels[channel]
