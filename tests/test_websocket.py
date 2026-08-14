from types import SimpleNamespace
from unittest.mock import MagicMock

from ouro.realtime.websocket import (
    CONNECT_WAIT_TIMEOUT_S,
    REFRESH_GAP_S,
    OuroWebSocket,
)


def _ws() -> OuroWebSocket:
    ouro = SimpleNamespace(
        websocket_url="https://example.test",
        access_token="token",
    )
    ws = OuroWebSocket(ouro)
    ws.sio = MagicMock()
    ws.sio.connected = True
    return ws


def test_connect_does_not_sleep():
    ws = _ws()
    ws.connect("token")
    ws.sio.connect.assert_called_once()
    kwargs = ws.sio.connect.call_args.kwargs
    assert kwargs["wait"] is True
    assert kwargs["wait_timeout"] == CONNECT_WAIT_TIMEOUT_S
    ws.sio.sleep.assert_not_called()


def test_ensure_connected_skips_when_already_connected():
    ws = _ws()
    ws.ensure_connected()
    ws.sio.connect.assert_not_called()


def test_ensure_connected_connects_when_down():
    ws = _ws()
    ws.sio.connected = False

    def _connect(*_args, **_kwargs):
        ws.sio.connected = True

    ws.sio.connect.side_effect = _connect
    ws.ensure_connected("token")
    ws.sio.connect.assert_called_once()
    ws.sio.disconnect.assert_not_called()
    ws.sio.sleep.assert_not_called()


def test_refresh_connection_uses_short_gap():
    ws = _ws()
    ws.refresh_connection("token")
    ws.sio.disconnect.assert_called_once()
    ws.sio.sleep.assert_called_once_with(REFRESH_GAP_S)
    ws.sio.connect.assert_called_once()
