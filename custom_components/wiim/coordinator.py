"""WiiM coordinator - minimal integration layer using pywiim."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from pywiim import Player, PollingStrategy, WiiMClient
from pywiim.exceptions import WiiMConnectionError, WiiMError, WiiMTimeoutError
from pywiim.upnp.eventer import UpnpEventer

_LOGGER = logging.getLogger(__name__)

# Resubscribe when UPnP events stay down this long (e.g. after a network outage).
UPNP_RESUBSCRIBE_AFTER = 120.0
UPNP_RESUBSCRIBE_COOLDOWN = 300.0
# pywiim counts a polled change as missed unless an event arrived within its grace
# period (2 s, sized for 1 s polling); at slower polling the poll trails the event.
UPNP_HEALTH_GRACE_MARGIN = 1.0
_PYWIIM_MISC_LOGGER_NAME = "pywiim.api.misc"
_LED_READ_FALLBACK_MESSAGE = "LED indicator read not available for device (no API or read failed); assuming on"
_EXPECTED_PYWIIM_LOG_FILTER_INSTALLED = False


class _ExpectedPywiimFallbackFilter(logging.Filter):
    """Suppress pywiim fallback warnings that are expected in HA polling."""

    _wiim_expected_pywiim_fallback_filter = True

    def filter(self, record: logging.LogRecord) -> bool:
        """Return False for known noisy pywiim fallback records."""
        return record.getMessage() != _LED_READ_FALLBACK_MESSAGE


def _install_expected_pywiim_log_filter() -> None:
    """Install log filtering for pywiim fallbacks that HA should not surface."""
    global _EXPECTED_PYWIIM_LOG_FILTER_INSTALLED

    if _EXPECTED_PYWIIM_LOG_FILTER_INSTALLED:
        return

    logger = logging.getLogger(_PYWIIM_MISC_LOGGER_NAME)
    if not any(getattr(log_filter, "_wiim_expected_pywiim_fallback_filter", False) for log_filter in logger.filters):
        logger.addFilter(_ExpectedPywiimFallbackFilter())
    _EXPECTED_PYWIIM_LOG_FILTER_INSTALLED = True


_UNREACHABLE_ERROR_MARKERS = (
    "device unreachable",
    "connection failed on all attempted protocols",
    "cannot connect to host",
    "connect call failed",
    "connection refused",
    "connection reset",
    "network is unreachable",
    "name or service not known",
)


def _is_expected_unreachable_error(err: Exception) -> bool:
    """Return True when error indicates expected offline/unreachable device."""
    if isinstance(err, (WiiMConnectionError, WiiMTimeoutError)):
        return True
    last_error = getattr(err, "last_error", None)
    if last_error is not None and last_error is not err and _is_expected_unreachable_error(last_error):
        return True
    # Powered-off devices often surface as WiiMRequestError after retries
    # ("Request failed after 2 attempts: Cannot connect to host"), not as
    # WiiMConnectionError. Treat those connect failures as unreachable too.
    err_text = str(err).lower()
    return any(marker in err_text for marker in _UNREACHABLE_ERROR_MARKERS)


def _compact_wiim_error(err: Exception) -> str:
    """Return compact error text to avoid log spam."""
    if _is_expected_unreachable_error(err):
        return "device unreachable"
    return str(err)


def _isolate_artwork_enrichment(player: Player) -> None:
    """Run pywiim artwork enrichment in its own task.

    pywiim cancels the task that ran the previous enrichment. That is the
    caller's task on the polling path, so an event-driven enrichment would
    otherwise cancel config entry setup or a coordinator refresh.
    """
    manager = getattr(player, "_coverart_mgr", None)
    original = getattr(manager, "enrich_metadata_on_track_change", None)
    if not asyncio.iscoroutinefunction(original):
        return

    async def isolated(*args: Any, **kwargs: Any) -> None:
        task = asyncio.get_running_loop().create_task(original(*args, **kwargs))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
            _LOGGER.debug("Artwork enrichment for %s superseded by a newer one", player.host)

    manager.enrich_metadata_on_track_change = isolated


def _drop_uri_track_source(player: Player) -> None:
    """Ignore UPnP TrackSource values that are content URIs, not source names.

    For Spotify the Ultra sends TrackSource="spotify:playlist:<id>". pywiim
    compares it with the polled source ("spotify"), treats it as a source
    switch and marks the new track's metadata as leftover, so polled titles
    are dropped until the next track. HTTP is the preferred source anyway.
    """
    sync = getattr(player, "_state_synchronizer", None)
    original = getattr(sync, "update_from_upnp", None)
    if original is None:
        return

    def filtered(data: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
        source = data.get("source") if isinstance(data, dict) else None
        if isinstance(source, str) and (":" in source or "/" in source):
            data = {k: v for k, v in data.items() if k != "source"}
        return original(data, *args, **kwargs)

    sync.update_from_upnp = filtered


class WiiMCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """WiiM coordinator - minimal glue between pywiim and Home Assistant."""

    def __init__(
        self,
        hass: HomeAssistant,
        host: str,
        entry=None,
        capabilities: dict[str, Any] | None = None,
        port: int | None = None,
        protocol: str | None = None,
        timeout: int = 10,
    ) -> None:
        """Initialize the coordinator."""
        _install_expected_pywiim_log_filter()
        super().__init__(
            hass,
            _LOGGER,
            name=f"WiiM {host}",
            update_interval=timedelta(seconds=5),  # Default, will adapt
        )
        self.hass = hass
        self.entry = entry
        self._capabilities = capabilities or {}

        # Get HA's shared aiohttp session (for connection pooling)
        session = async_get_clientsession(hass)

        # Create pywiim client with HA's session
        # Only pass port/protocol if we have a cached endpoint (optimized pattern)
        # Otherwise, let pywiim probe automatically (simplest pattern)
        client_kwargs = {
            "host": host,
            "timeout": timeout,
            "session": session,
            "capabilities": capabilities,
        }
        if port is not None and protocol is not None:
            # We have a cached endpoint - use it for faster startup
            client_kwargs["port"] = port
            client_kwargs["protocol"] = protocol
        # If port/protocol not provided, pywiim will probe automatically

        client = WiiMClient(**client_kwargs)

        # Wrap client in Player (recommended for HA - pywiim manages all state)
        # pywiim 2.1.70+ handles player linking internally via its player registry

        # We need to include player_finder and all_players_finder to enable cross-coordinator group linking.
        # In pywiim's player/groupsops.py the "Case 2" for device is master but we don't have a group object
        # short-circuits if there isn't a player_finder callback.
        # all_players_finder is also included as a final fallback in case the UUID lookup doesn't work for
        # some reason.

        self.player = Player(
            client,
            on_state_changed=self._on_player_state_changed,
            player_finder=self._player_finder,
            all_players_finder=self._all_players_finder,
        )
        _isolate_artwork_enrichment(self.player)
        _drop_uri_track_source(self.player)

        # Use pywiim's PollingStrategy to determine when to poll
        self._polling_strategy = PollingStrategy(self._capabilities) if self._capabilities else PollingStrategy({})
        self._refresh_in_progress = False
        self._upnp_eventer: UpnpEventer | None = None
        self._upnp_wanted = False
        self._upnp_stalled_since: float | None = None
        self._upnp_last_restart = 0.0

    def update_capabilities(self, capabilities: dict[str, Any]) -> None:
        """Apply a refreshed capabilities mapping (e.g. after firmware change).

        Config entry data is updated separately in ``__init__``; this keeps the
        coordinator, adaptive polling, and the pywiim client flags in sync.
        """
        merged = dict(capabilities)
        self._capabilities.clear()
        self._capabilities.update(merged)
        client_caps = getattr(self.player.client, "_capabilities", None)
        if client_caps is not None and client_caps is not self._capabilities:
            client_caps.clear()
            client_caps.update(merged)
        self._polling_strategy = PollingStrategy(self._capabilities) if self._capabilities else PollingStrategy({})

    def _player_finder(self, host_or_uuid: str) -> Player | None:
        """Find a Player object across all coordinators by host IP or UUID.

        Called by pywiim when it needs to resolve a slave's IP/UUID (from
        getSlaveList) to an actual Player object for group linking.
        """
        from .data import get_all_coordinators

        for coordinator in get_all_coordinators(self.hass):
            if coordinator is self:
                continue
            try:
                p = coordinator.player
                if getattr(p, "host", None) == host_or_uuid:
                    return p
                if getattr(p, "uuid", None) == host_or_uuid:
                    return p
            except Exception as err:
                _LOGGER.debug("Error in player_finder for %s: %s", host_or_uuid, _compact_wiim_error(err))
        return None

    def _all_players_finder(self) -> list[Player]:
        """Return all Player objects from every registered coordinator.

        Called by pywiim to infer slave role if e.g. a device is still reporting
        that it's solo even though it appears in another device's getSlaveList.
        """
        from .data import get_all_coordinators

        players = []
        for c in get_all_coordinators(self.hass):
            try:
                players.append(c.player)
            except Exception as err:
                _LOGGER.debug("Error in all_players_finder: %s", _compact_wiim_error(err))
        return players

    @callback
    def _on_player_state_changed(self) -> None:
        """Callback when pywiim Player detects state changes.

        Directly notifies listeners to update immediately without going through
        the coordinator's data update mechanism (which has throttling/debouncing).
        The callback fires AFTER pywiim has fully updated the Player object's
        properties (including metadata), so entities can read fresh data directly
        from self.player.
        """
        # Update coordinator's cached data reference (but don't trigger update flow)
        # This ensures self.data is always in sync with self.player
        self.data = {"player": self.player}

        # Directly notify all entities to refresh their state from the player
        # This bypasses DataUpdateCoordinator's throttling for immediate UI updates,
        # except while the coordinator is already performing a timed refresh. In
        # that case DataUpdateCoordinator publishes the completed refresh once.
        if self._refresh_in_progress:
            return
        self.async_update_listeners()

    @property
    def upnp_push_active(self) -> bool:
        """Return True while UPnP events from the device are arriving."""
        eventer = self._upnp_eventer
        if eventer is None or eventer.check_available:
            return False
        return getattr(self.player, "upnp_is_healthy", None) is not False

    async def async_start_upnp_events(self) -> None:
        """Subscribe to the device's UPnP events. Polling remains the fallback."""
        self._upnp_wanted = True
        await self._async_subscribe_upnp()

    async def _async_subscribe_upnp(self) -> None:
        if self._upnp_eventer is not None:
            return
        eventer: UpnpEventer | None = None
        try:
            if not await self.player._ensure_upnp_client():
                return
            eventer = UpnpEventer(
                self.player._upnp_client,
                self.player,
                getattr(self.player, "uuid", None) or self.player.host,
                state_updated_callback=self._on_upnp_event,
            )
            await eventer.start()
        except Exception as err:  # noqa: BLE001
            _LOGGER.info("UPnP events unavailable for %s, using polling: %s", self.player.host, err)
            if eventer is not None:
                try:
                    await eventer.async_unsubscribe()
                except Exception:  # noqa: BLE001
                    pass
            return
        self._upnp_eventer = eventer
        self.player._upnp_eventer = eventer
        _LOGGER.info("UPnP event subscription active for %s", self.player.host)

    async def async_stop_upnp_events(self) -> None:
        """Unsubscribe from UPnP events."""
        self._upnp_wanted = False
        await self._async_unsubscribe_upnp()

    async def _async_unsubscribe_upnp(self) -> None:
        eventer, self._upnp_eventer = self._upnp_eventer, None
        if eventer is None:
            return
        if getattr(self.player, "_upnp_eventer", None) is eventer:
            self.player._upnp_eventer = None
        try:
            await eventer.async_unsubscribe()
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Error stopping UPnP events for %s: %s", self.player.host, err)

    async def _async_check_upnp_subscription(self) -> None:
        """Resubscribe when events stopped and auto-resubscribe did not recover.

        pywiim leaves ``check_available`` set until an event arrives, so a
        subscription lost in a network outage otherwise stays on 1 s polling.
        """
        if not self._upnp_wanted:
            return
        if self.upnp_push_active:
            self._upnp_stalled_since = None
            return
        now = time.monotonic()
        if self._upnp_stalled_since is None:
            self._upnp_stalled_since = now
            return
        if now - self._upnp_stalled_since < UPNP_RESUBSCRIBE_AFTER:
            return
        if now - self._upnp_last_restart < UPNP_RESUBSCRIBE_COOLDOWN:
            return
        self._upnp_last_restart = now
        self._upnp_stalled_since = None
        _LOGGER.info("UPnP events from %s stopped; resubscribing", self.player.host)
        await self._async_unsubscribe_upnp()
        await self._async_subscribe_upnp()
        tracker = getattr(self.player, "_upnp_health_tracker", None)
        if tracker is not None:
            tracker.reset_statistics()

    def _match_upnp_health_grace(self) -> None:
        """Let pywiim's miss detection wait as long as the poll that checks it."""
        tracker = getattr(self.player, "_upnp_health_tracker", None)
        grace = getattr(tracker, "_grace_period", None)
        if not isinstance(grace, (int, float)):
            return
        interval = self.update_interval.total_seconds() if self.update_interval else 5.0
        tracker._grace_period = max(grace, interval + UPNP_HEALTH_GRACE_MARGIN)

    @callback
    def _on_upnp_event(self) -> None:
        """Push UPnP-driven state changes to entities."""
        self._on_player_state_changed()

    async def _async_update_data(self) -> dict[str, Any]:
        """Update coordinator data - polls device following pywiim's PollingStrategy."""
        try:
            # Call player.refresh() to poll device and update cached state
            # PollingStrategy determines WHEN to poll (adaptive intervals)
            self._refresh_in_progress = True
            self._match_upnp_health_grace()
            try:
                await self.player.refresh()
            finally:
                self._refresh_in_progress = False

            await self._async_check_upnp_subscription()

            # Update polling interval using pywiim's PollingStrategy
            role = self.player.role
            is_playing = self.player.is_playing  # pywiim v2.1.37+ provides bool directly
            optimal_interval = self._polling_strategy.get_optimal_interval(role, is_playing)
            # Events cover track and state changes, so the 1 s playing poll is not needed.
            if self.upnp_push_active:
                optimal_interval = max(optimal_interval, PollingStrategy.NORMAL_POLL_INTERVAL)
            current_interval = self.update_interval.total_seconds() if self.update_interval else 5.0
            if current_interval != optimal_interval:
                self.update_interval = timedelta(seconds=optimal_interval)

            # Return Player object - it has everything (state, metadata, group info, etc.)
            if is_playing and _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug(
                    "Poll result for %s: state=%s, pos=%s, dur=%s, title='%s'",
                    self.player.host,
                    self.player.play_state,
                    self.player.media_position,
                    self.player.media_duration,
                    self.player.media_title,
                )

            result = {"player": self.player}
            return result

        except WiiMError as err:
            if _is_expected_unreachable_error(err):
                _LOGGER.debug("Update failed for %s: %s", self.player.host, _compact_wiim_error(err))
                # Powered-off / unreachable devices must go unavailable so automations
                # that wait on availability (e.g. smart-plug scripts) can proceed.
                raise UpdateFailed(
                    f"Failed to communicate with {self.player.host}: {_compact_wiim_error(err)}"
                ) from err
            _LOGGER.warning("Update failed for %s: %s", self.player.host, _compact_wiim_error(err))
            # Return cached Player object for non-connectivity errors (parse blips, etc.)
            if self.data:
                return self.data
            raise UpdateFailed(f"Failed to communicate with {self.player.host}: {_compact_wiim_error(err)}") from err
