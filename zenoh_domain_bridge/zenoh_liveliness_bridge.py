import itertools
import logging
from dataclasses import replace
from typing import Callable

import zenoh

from .bridge_config import BridgeConfig
from .liveliness_token import LivelinessToken

logger = logging.getLogger(__name__)


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
        self._bridged_tokens: dict[LivelinessToken, zenoh.LivelinessToken] = {}
        self._node_liveliness_token = self._announce_bridge()
        self._initialize_liveliness()

    def _announce_bridge(self) -> zenoh.LivelinessToken:
        token = LivelinessToken(
            domain_id=self._config.global_domain_id,
            session_id=str(self._session.zid()),
            node_id="0",
            entity_id="0",
            entity_kind=LivelinessToken.EntityKind.NODE.value,
            mangled_enclave="%",
            mangled_namespace="%",
            node_name=self._config.bridge_name,
        )
        logger.debug(f"Announcing domain bridge via node token '{token.key_expr}'...")
        return self._session.liveliness().declare_token(token.key_expr)

    def _initialize_liveliness(self) -> None:
        local_key_expr = f"@ros2_lv/{self._config.local_domain_id}/**"
        global_key_expr = f"@ros2_lv/{self._config.global_domain_id}/**"
        logger.info(
            f"Listening to tokens on '{local_key_expr}' and '{global_key_expr}'..."
        )
        self._session.liveliness().declare_subscriber(
            local_key_expr,
            self._handle_token,
        )
        self._session.liveliness().declare_subscriber(
            global_key_expr,
            self._handle_token,
        )

        logger.info(
            f"Querying existing tokens on '{local_key_expr}' and '{global_key_expr}'..."
        )
        for reply in itertools.chain(
            self._session.liveliness().get(local_key_expr),
            self._session.liveliness().get(global_key_expr),
        ):
            sample = reply.result

            if isinstance(sample, zenoh.Sample):
                self._handle_token(sample)

    def _handle_token(
        self,
        sample: zenoh.Sample,
    ) -> None:
        token = LivelinessToken.from_key_expr(str(sample.key_expr))

        # We do not bridge node tokens.
        if token.kind == LivelinessToken.EntityKind.NODE:
            return

        assert token.mangled_fqn is not None

        # We only bridge communication tokens that start with correct prefix.
        if not token.mangled_fqn.startswith(self._config.mangled_bridged_fqn_prefix):
            return

        # We do not bridge tokens that were created by this bridge.
        if token.node_name == self._config.bridge_name:
            return

        # We need to bridge this token from the local to the global domain.
        target_domain_id = (
            self._config.global_domain_id
            if token.domain_id == self._config.local_domain_id
            else self._config.local_domain_id
        )

        match sample.kind:
            case zenoh.SampleKind.PUT:
                self._bridge_token(token, target_domain_id)
            case zenoh.SampleKind.DELETE:
                self._unbridge_token(token, target_domain_id)

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
        logger.debug(
            f"Created bridged token '{bridged_token.key_expr}' because token '{token.key_expr}' was created."
        )

        if self._on_token_bridged:
            self._on_token_bridged(token)

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
        logger.debug(
            f"Deleted bridged token '{bridged_token.key_expr}' because token '{token.key_expr}' was deleted."
        )

        if self._on_token_unbridged:
            self._on_token_unbridged(token)
