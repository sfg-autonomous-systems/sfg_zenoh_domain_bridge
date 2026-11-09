from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import replace
from queue import Empty, SimpleQueue
from threading import Event

import zenoh

from .bridge_config import BridgeConfig
from .liveliness_token import LivelinessToken

logger = logging.getLogger(__name__.split(".")[-1])


class ZenohLivelinessBridge:
    def __init__(
        self,
        session: zenoh.Session,
        config: BridgeConfig,
        on_token_bridged: Callable[[LivelinessToken], None] | None = None,
        on_token_unbridged: Callable[[LivelinessToken], None] | None = None,
    ) -> None:
        self._session = session
        self._config = config
        self._on_token_bridged = on_token_bridged
        self._on_token_unbridged = on_token_unbridged
        self._local_key_expr = (
            f"{LivelinessToken.key_expr_prefix}/{self._config.local_domain_id}/**"
        )
        self._global_key_expr = (
            f"{LivelinessToken.key_expr_prefix}/{self._config.global_domain_id}/**"
        )
        self._bridged_tokens: dict[LivelinessToken, zenoh.LivelinessToken] = {}
        self._pending_tokens: SimpleQueue[zenoh.Sample] = SimpleQueue()
        self._node_liveliness_token = self._announce_bridge()
        self._initialize_liveliness()

    def sync_liveliness(self) -> None:
        dones: list[Event] = []
        replies: list[zenoh.Reply] = []

        for key_expr in (self._local_key_expr, self._global_key_expr):
            done = Event()
            dones.append(done)

            # Zenoh delivers known tokens on the calling thread while holding the
            # session state lock, so get() deadlocked once its default 256-slot channel
            # was full. Collect replies through a callback instead, and handle them
            # only after both queries have ended.
            self._session.liveliness().get(
                key_expr,
                zenoh.handlers.Callback(replies.append, done.set),
            )

        for done in dones:
            done.wait()

        tokens_to_unbridge = set(self._bridged_tokens)

        for reply in replies:
            sample = reply.result

            if not isinstance(sample, zenoh.Sample):
                logger.warning(f"Liveliness query failed: {sample.payload.to_string()}")
                continue

            token = self._handle_token(sample)

            if token is not None:
                tokens_to_unbridge.discard(token)

        # Unbridge stale tokens that were not returned by the queries.
        for token in tokens_to_unbridge:
            self._unbridge_token(token, self._get_target_domain_id(token))

    def handle_pending_tokens(self, timeout: float = 0.1) -> None:
        while True:
            try:
                self._handle_token(self._pending_tokens.get(timeout=timeout))
            except Empty:
                break

    def _announce_bridge(self) -> zenoh.LivelinessToken:
        token = LivelinessToken(
            domain_id=self._config.global_domain_id,
            session_id=str(self._session.zid()),
            node_id="0",
            entity_id="0",
            entity_kind=LivelinessToken.EntityKind.Node.value,
            mangled_enclave="%",
            mangled_namespace="%",
            node_name=self._config.bridge_name,
        )
        logger.debug(f"Created node token: {token}")
        return self._session.liveliness().declare_token(token.key_expr)

    def _initialize_liveliness(self) -> None:
        logger.info(
            f"Listening to tokens on '{self._local_key_expr}' and '{self._global_key_expr}'..."
        )
        self._session.liveliness().declare_subscriber(
            self._local_key_expr,
            self._pending_tokens.put,
        )
        self._session.liveliness().declare_subscriber(
            self._global_key_expr,
            self._pending_tokens.put,
        )

    def _get_target_domain_id(self, token: LivelinessToken) -> int:
        if token.domain_id == self._config.local_domain_id:
            return self._config.global_domain_id

        return self._config.local_domain_id

    def _handle_token(
        self,
        sample: zenoh.Sample,
    ) -> LivelinessToken | None:
        try:
            token = LivelinessToken.from_key_expr(str(sample.key_expr))
        except ValueError as error:
            logger.error(error)
            return None

        # We do not bridge node tokens.
        if token.kind == LivelinessToken.EntityKind.Node:
            return None

        # We do not bridge tokens that were created by this bridge.
        if token.node_name == self._config.bridge_name:
            return None

        assert token.mangled_fqn is not None

        # We only bridge communication tokens that match the user-provided patterns.
        if not any(
            pattern.fullmatch(token.mangled_fqn)
            for pattern in self._config.bridged_mangled_fqn_patterns
        ):
            return None

        match sample.kind:
            case zenoh.SampleKind.PUT:
                self._bridge_token(token, self._get_target_domain_id(token))
            case zenoh.SampleKind.DELETE:
                self._unbridge_token(token, self._get_target_domain_id(token))

        return token

    def _bridge_token(
        self,
        token: LivelinessToken,
        domain_id_to_bridge_to: int,
    ) -> None:
        if token in self._bridged_tokens:
            return

        bridged_token = replace(
            token,
            domain_id=domain_id_to_bridge_to,
            session_id=str(self._session.zid()),
            node_id="0",
            entity_id="0",
            mangled_enclave="%",
            mangled_namespace="%",
            node_name=self._config.bridge_name,
        )

        self._bridged_tokens[token] = self._session.liveliness().declare_token(
            bridged_token.key_expr
        )
        logger.debug(f"Bridged token: {token} -> {bridged_token}")

        if self._on_token_bridged:
            try:
                self._on_token_bridged(token)
            except Exception as error:  # noqa: BLE001
                logger.error(f"Error in callback for token '{token}': {error}")

    def _unbridge_token(
        self,
        token: LivelinessToken,
        domain_id_to_unbridge_from: int,
    ) -> None:
        if token not in self._bridged_tokens:
            return

        bridged_token = replace(
            token,
            domain_id=domain_id_to_unbridge_from,
            session_id=str(self._session.zid()),
            node_id="0",
            entity_id="0",
            mangled_enclave="%",
            mangled_namespace="%",
            node_name=self._config.bridge_name,
        )

        self._bridged_tokens[token].undeclare()
        del self._bridged_tokens[token]
        logger.debug(f"Unbridged token: {token} -> {bridged_token}")

        if self._on_token_unbridged:
            try:
                self._on_token_unbridged(token)
            except Exception as error:  # noqa: BLE001
                logger.error(f"Error in callback for token '{token}': {error}")
