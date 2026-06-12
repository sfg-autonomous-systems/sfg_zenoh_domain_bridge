import itertools
import logging
import os
from dataclasses import replace

import zenoh

from zenoh_domain_bridge.liveliness.liveliness_token import LivelinessToken

logger = logging.getLogger(__name__)
local_domain_id = int(os.getenv("ROS_DOMAIN_ID", "0"))
global_domain_id = 0


class LivelinessBridger:
    def __init__(self, session: zenoh.Session, node_name: str):
        self._session = session
        self._node_name = node_name
        self._bridged_tokens: dict[LivelinessToken, zenoh.LivelinessToken] = {}

        key = LivelinessToken(
            domain_id=global_domain_id,
            session_id=str(session.id.zid),
            node_id=0,
            entity_id=0,
            entity_kind=LivelinessToken.EntityKind.NODE.value,
            mangled_enclave="%",
            mangled_namespace="%global%",
            node_name=self._node_name,
        )
        logger.debug(f"Announcing domain bridge via node token '{key.to_key()}'...")
        self._node_liveliness_token = session.liveliness().declare_token(key.to_key())

        local_key_expr = f"@ros2_lv/{local_domain_id}/**"
        global_key_expr = f"@ros2_lv/{global_domain_id}/**"
        logger.info(
            f"Listening to tokens on '{local_key_expr}' and '{global_key_expr}'..."
        )
        session.liveliness().declare_subscriber(
            local_key_expr,
            self._handle_token,
        )
        session.liveliness().declare_subscriber(
            global_key_expr,
            self._handle_token,
        )

        logger.info(
            f"Querying existing tokens on '{local_key_expr}' and '{global_key_expr}'..."
        )
        for reply in itertools.chain(
            session.liveliness().get(local_key_expr),
            session.liveliness().get(global_key_expr),
        ):
            sample = reply.result

            if isinstance(sample, zenoh.Sample):
                self._handle_token(sample)

    def _bridge_token(
        self,
        token: LivelinessToken,
        domain_id_to_bridge_to: str,
    ) -> None:
        if token in self._bridged_tokens:
            return

        bridged_token = replace(
            token,
            domain_id=domain_id_to_bridge_to,
            session_id=str(self._session.id.zid),
            node_id=0,
            entity_id=0,
            mangled_enclave="%",
            mangled_namespace="%global%",
            node_name=self._node_name,
        )

        self._bridged_tokens[token] = self._session.liveliness().declare_token(
            bridged_token.to_key()
        )
        logger.debug(
            f"Created global token '{bridged_token.to_key()}' because local token '{token.to_key()}' was created."
        )

    def _unbridge_token(
        self,
        token: LivelinessToken,
        domain_id_to_unbridge_from: str,
    ) -> None:
        if token not in self._bridged_tokens:
            return

        bridged_token = replace(
            token,
            domain_id=domain_id_to_unbridge_from,
            session_id=str(self._session.id.zid),
            node_id=0,
            entity_id=0,
            mangled_enclave="%",
            mangled_namespace="%global%",
            node_name=self._node_name,
        )

        del self._bridged_tokens[token]
        logger.debug(
            f"Deleted global token '{bridged_token.to_key()}' because local token '{token.to_key()}' was deleted."
        )

    def _handle_token(
        self,
        sample: zenoh.Sample,
    ) -> None:
        token = LivelinessToken.from_key(str(sample.key_expr))

        # We do not bridge node tokens.
        if token.kind == LivelinessToken.EntityKind.NODE:
            return

        # We only bridge communication tokens that start with /global.
        if not token.mangled_qualified_name.startswith("%global%"):
            return

        # We do not bridge tokens that were created by this bridge.
        if token.node_name == self._node_name:
            return

        # We need to bridge this token from the local to the global domain.
        target_domain_id = global_domain_id if token.is_local else local_domain_id

        match sample.kind:
            case zenoh.SampleKind.PUT:
                self._bridge_token(token, target_domain_id)
            case zenoh.SampleKind.DELETE:
                self._unbridge_token(token, target_domain_id)
