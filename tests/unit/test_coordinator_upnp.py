"""UPnP event subscription tests for the WiiM coordinator."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pywiim import PollingStrategy

from custom_components.wiim import coordinator as coordinator_module
from custom_components.wiim.const import DOMAIN
from custom_components.wiim.coordinator import WiiMCoordinator
from tests.const import MOCK_CONFIG


@pytest.fixture
def player():
    """Mock pywiim Player with a UPnP client."""
    player = MagicMock()
    player.host = "192.168.1.100"
    player.uuid = "uuid-1"
    player.role = "solo"
    player.is_playing = True
    player.upnp_is_healthy = True
    player.refresh = AsyncMock()
    player._ensure_upnp_client = AsyncMock(return_value=True)
    player._upnp_client = MagicMock()
    player._upnp_eventer = None
    return player


@pytest.fixture
def eventer():
    """Mock UpnpEventer instance."""
    eventer = MagicMock()
    eventer.start = AsyncMock()
    eventer.async_unsubscribe = AsyncMock()
    eventer.check_available = False
    return eventer


@pytest.fixture
async def coordinator(hass, player):
    """Coordinator wired to the mock player."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG)
    with (
        patch.object(coordinator_module, "WiiMClient"),
        patch.object(coordinator_module, "Player", return_value=player),
    ):
        return WiiMCoordinator(hass, host=player.host, entry=entry, capabilities={})


async def test_start_subscribes_and_marks_push_active(coordinator, player, eventer) -> None:
    """A successful subscription stores the eventer and enables push mode."""
    with patch.object(coordinator_module, "UpnpEventer", return_value=eventer) as cls:
        await coordinator.async_start_upnp_events()

    cls.assert_called_once()
    assert cls.call_args.args[:3] == (player._upnp_client, player, "uuid-1")
    eventer.start.assert_awaited_once()
    assert player._upnp_eventer is eventer
    assert coordinator.upnp_push_active is True


async def test_start_is_idempotent(coordinator, eventer) -> None:
    """A second start does not subscribe again."""
    with patch.object(coordinator_module, "UpnpEventer", return_value=eventer) as cls:
        await coordinator.async_start_upnp_events()
        await coordinator.async_start_upnp_events()
    assert cls.call_count == 1


async def test_no_upnp_client_keeps_polling(coordinator, player) -> None:
    """Without a UPnP client nothing is subscribed."""
    player._ensure_upnp_client.return_value = False
    with patch.object(coordinator_module, "UpnpEventer") as cls:
        await coordinator.async_start_upnp_events()
    cls.assert_not_called()
    assert coordinator.upnp_push_active is False


async def test_failed_subscription_cleans_up(coordinator, eventer) -> None:
    """A failed subscription unwinds the notify server and keeps polling."""
    eventer.start.side_effect = OSError("no route")
    with patch.object(coordinator_module, "UpnpEventer", return_value=eventer):
        await coordinator.async_start_upnp_events()
    eventer.async_unsubscribe.assert_awaited_once()
    assert coordinator.upnp_push_active is False


@pytest.mark.parametrize(
    ("check_available", "healthy", "expected"), [(True, True, False), (False, False, False), (False, None, True)]
)
async def test_push_active_rules(coordinator, player, eventer, check_available, healthy, expected) -> None:
    """Push mode ends when resubscription fails or polling sees missed changes."""
    with patch.object(coordinator_module, "UpnpEventer", return_value=eventer):
        await coordinator.async_start_upnp_events()
    eventer.check_available = check_available
    player.upnp_is_healthy = healthy
    assert coordinator.upnp_push_active is expected


async def test_playing_interval_relaxes_only_with_push(coordinator, eventer) -> None:
    """Playback polls every second without push and every 5 s with push."""
    await coordinator._async_update_data()
    assert coordinator.update_interval == timedelta(seconds=PollingStrategy.FAST_POLL_INTERVAL)

    with patch.object(coordinator_module, "UpnpEventer", return_value=eventer):
        await coordinator.async_start_upnp_events()
    await coordinator._async_update_data()
    assert coordinator.update_interval == timedelta(seconds=PollingStrategy.NORMAL_POLL_INTERVAL)

    eventer.check_available = True
    await coordinator._async_update_data()
    assert coordinator.update_interval == timedelta(seconds=PollingStrategy.FAST_POLL_INTERVAL)


async def test_upnp_event_notifies_entities(coordinator, eventer) -> None:
    """Events push the player state to entities."""
    listener = MagicMock()
    unsub = coordinator.async_add_listener(listener)
    coordinator._on_upnp_event()
    unsub()
    listener.assert_called_once()


async def test_stop_unsubscribes(coordinator, player, eventer) -> None:
    """Stopping unsubscribes once and clears the player's eventer."""
    with patch.object(coordinator_module, "UpnpEventer", return_value=eventer):
        await coordinator.async_start_upnp_events()
    await coordinator.async_stop_upnp_events()
    await coordinator.async_stop_upnp_events()
    eventer.async_unsubscribe.assert_awaited_once()
    assert player._upnp_eventer is None
    assert coordinator.upnp_push_active is False


async def _stall(coordinator, eventer, start: float) -> None:
    with patch.object(coordinator_module, "UpnpEventer", return_value=eventer):
        await coordinator.async_start_upnp_events()
    eventer.check_available = True
    with patch.object(coordinator_module.time, "monotonic", return_value=start):
        await coordinator._async_update_data()


async def test_stalled_subscription_resubscribes(coordinator, player, eventer) -> None:
    """Events that stay down past the delay trigger one fresh subscription."""
    await _stall(coordinator, eventer, 1000.0)
    fresh = MagicMock(start=AsyncMock(), async_unsubscribe=AsyncMock(), check_available=False)

    with (
        patch.object(coordinator_module, "UpnpEventer", return_value=fresh),
        patch.object(coordinator_module.time, "monotonic", return_value=1000.0 + 60),
    ):
        await coordinator._async_update_data()
    eventer.async_unsubscribe.assert_not_awaited()

    with (
        patch.object(coordinator_module, "UpnpEventer", return_value=fresh),
        patch.object(
            coordinator_module.time, "monotonic", return_value=1000.0 + coordinator_module.UPNP_RESUBSCRIBE_AFTER
        ),
    ):
        await coordinator._async_update_data()

    eventer.async_unsubscribe.assert_awaited_once()
    fresh.start.assert_awaited_once()
    player._upnp_health_tracker.reset_statistics.assert_called_once()
    assert coordinator.upnp_push_active is True
    assert coordinator.update_interval == timedelta(seconds=PollingStrategy.NORMAL_POLL_INTERVAL)


async def test_resubscribe_cooldown(coordinator, eventer) -> None:
    """A subscription that keeps failing is retried at most once per cooldown."""
    coordinator._upnp_last_restart = 1000.0
    await _stall(coordinator, eventer, 1000.0)
    with patch.object(coordinator_module.time, "monotonic", return_value=1000.0 + 200):
        await coordinator._async_update_data()
    eventer.async_unsubscribe.assert_not_awaited()


async def test_failed_start_is_retried(coordinator, player) -> None:
    """A subscription that failed at setup (e.g. network down) is retried later."""
    player._ensure_upnp_client = AsyncMock(return_value=False)
    await coordinator.async_start_upnp_events()
    with patch.object(coordinator_module.time, "monotonic", return_value=1000.0):
        await coordinator._async_update_data()

    player._ensure_upnp_client = AsyncMock(return_value=True)
    fresh = MagicMock(start=AsyncMock(), async_unsubscribe=AsyncMock(), check_available=False)
    with (
        patch.object(coordinator_module, "UpnpEventer", return_value=fresh),
        patch.object(coordinator_module.time, "monotonic", return_value=2000.0),
    ):
        await coordinator._async_update_data()
    fresh.start.assert_awaited_once()


async def test_no_resubscribe_after_stop(coordinator, eventer) -> None:
    """Unloading stops the watchdog."""
    await _stall(coordinator, eventer, 1000.0)
    await coordinator.async_stop_upnp_events()
    with (
        patch.object(coordinator_module, "UpnpEventer") as cls,
        patch.object(coordinator_module.time, "monotonic", return_value=5000.0),
    ):
        await coordinator._async_update_data()
    cls.assert_not_called()
