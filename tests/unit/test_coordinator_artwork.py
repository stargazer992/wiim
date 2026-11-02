"""Artwork enrichment must not cancel the task that awaits it."""

import asyncio
from types import SimpleNamespace

from custom_components.wiim.coordinator import _isolate_artwork_enrichment


class _CoverArtManager:
    """Mimics pywiim: cancels the task that ran the previous enrichment."""

    def __init__(self) -> None:
        self._artwork_fetch_task = None
        self.completed = 0

    async def enrich_metadata_on_track_change(self, merged_state, *, track_changed=None):
        current = asyncio.current_task()
        previous = self._artwork_fetch_task
        if previous and not previous.done() and previous is not current:
            previous.cancel()
        self._artwork_fetch_task = current
        await asyncio.sleep(0.05)
        self.completed += 1


def _player(manager):
    return SimpleNamespace(host="192.168.1.100", _coverart_mgr=manager)


async def test_unpatched_manager_cancels_caller() -> None:
    """Documents the pywiim behaviour the patch works around."""
    manager = _CoverArtManager()
    caller = asyncio.create_task(manager.enrich_metadata_on_track_change({}))
    await asyncio.sleep(0)
    asyncio.create_task(manager.enrich_metadata_on_track_change({}))
    await asyncio.sleep(0.1)
    assert caller.cancelled()


async def test_superseded_enrichment_does_not_cancel_caller() -> None:
    manager = _CoverArtManager()
    _isolate_artwork_enrichment(_player(manager))

    caller = asyncio.create_task(manager.enrich_metadata_on_track_change({}))
    await asyncio.sleep(0.01)
    newer = asyncio.create_task(manager.enrich_metadata_on_track_change({}, track_changed=True))
    await asyncio.gather(caller, newer)

    assert not caller.cancelled()
    assert manager.completed == 1


async def test_cancelling_caller_still_propagates() -> None:
    manager = _CoverArtManager()
    _isolate_artwork_enrichment(_player(manager))

    caller = asyncio.create_task(manager.enrich_metadata_on_track_change({}))
    await asyncio.sleep(0.01)
    caller.cancel()
    await asyncio.gather(caller, return_exceptions=True)
    assert caller.cancelled()
    await asyncio.sleep(0.1)
    assert manager.completed == 1


async def test_errors_propagate() -> None:
    class _Failing:
        async def enrich_metadata_on_track_change(self, merged_state, *, track_changed=None):
            raise ValueError("boom")

    manager = _Failing()
    _isolate_artwork_enrichment(_player(manager))
    try:
        await manager.enrich_metadata_on_track_change({})
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_non_coroutine_manager_left_alone() -> None:
    manager = SimpleNamespace(enrich_metadata_on_track_change=lambda *a, **k: None)
    original = manager.enrich_metadata_on_track_change
    _isolate_artwork_enrichment(_player(manager))
    assert manager.enrich_metadata_on_track_change is original
