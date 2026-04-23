"""In-memory metagraph cache — refreshed periodically from the chain."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from greencompute_protocol import MetagraphEntry

logger = logging.getLogger(__name__)


class MetagraphCache:
    """Holds the latest metagraph snapshot in memory."""

    def __init__(self) -> None:
        self._entries: dict[str, MetagraphEntry] = {}  # hotkey -> entry
        self._by_uid: dict[int, MetagraphEntry] = {}
        self.last_synced_at: datetime | None = None

    def update(self, entries: list[MetagraphEntry]) -> None:
        self._entries = {e.hotkey: e for e in entries}
        self._by_uid = {e.uid: e for e in entries}
        self.last_synced_at = datetime.now(UTC)
        logger.info("metagraph cache updated: %d entries", len(entries))

    def is_registered(self, hotkey: str) -> bool:
        return hotkey in self._entries

    def get_by_hotkey(self, hotkey: str) -> MetagraphEntry | None:
        return self._entries.get(hotkey)

    def get_by_uid(self, uid: int) -> MetagraphEntry | None:
        return self._by_uid.get(uid)

    def list_entries(self) -> list[MetagraphEntry]:
        return list(self._entries.values())

    def hotkey_to_uid(self, hotkey: str) -> int | None:
        entry = self._entries.get(hotkey)
        return entry.uid if entry else None

    @property
    def size(self) -> int:
        return len(self._entries)
