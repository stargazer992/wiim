"""UPnP TrackSource URIs must not blank the polled title."""

from types import SimpleNamespace

from pywiim.state import StateSynchronizer

from custom_components.wiim.coordinator import _drop_uri_track_source

TITLE = "Late Night Talking - Piano Version"


def _poll(sync: StateSynchronizer) -> None:
    sync.update_from_http({"play_state": "play", "source": "spotify", "title": TITLE, "artist": "Flying Fingers"})


def _event(sync: StateSynchronizer, source: str) -> None:
    sync.update_from_upnp({"play_state": "play", "source": source, "title": TITLE, "artist": "Flying Fingers"})


def test_unpatched_spotify_uri_blanks_polled_title() -> None:
    """Documents the pywiim behaviour the patch works around."""
    sync = StateSynchronizer()
    _poll(sync)
    _event(sync, "spotify:playlist:476A0b7udnakya2C5geTUD")
    sync.update_from_upnp({"title": None, "artist": None})
    _poll(sync)
    assert sync.get_merged_state().get("title") is None


def test_spotify_uri_keeps_polled_title() -> None:
    sync = StateSynchronizer()
    _drop_uri_track_source(SimpleNamespace(_state_synchronizer=sync))
    _poll(sync)
    _event(sync, "spotify:playlist:476A0b7udnakya2C5geTUD")
    sync.update_from_upnp({"title": None, "artist": None})
    _poll(sync)
    assert sync.get_merged_state().get("title") == TITLE
    assert sync.get_merged_state().get("source") == "spotify"


def test_plain_source_names_pass_through() -> None:
    seen = []
    sync = SimpleNamespace(update_from_upnp=lambda data, *a, **k: seen.append(data))
    _drop_uri_track_source(SimpleNamespace(_state_synchronizer=sync))
    sync.update_from_upnp({"source": "airplay", "title": "x"})
    sync.update_from_upnp({"source": "spotify:track:1", "title": "y"}, timestamp=1.0)
    assert seen == [{"source": "airplay", "title": "x"}, {"title": "y"}]


def test_missing_synchronizer_is_ignored() -> None:
    _drop_uri_track_source(SimpleNamespace())
