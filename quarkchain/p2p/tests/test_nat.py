import asyncio
import socket
import sys
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock

# CancelToken uses deprecated asyncio.Event(loop=loop) removed in py3.10+,
# patch it to ignore the loop kwarg
import quarkchain.p2p.cancel_token.token as _ct

_OrigCancelToken = _ct.CancelToken


class _PatchedCancelToken(_OrigCancelToken):
    def __init__(self, name, loop=None):
        self.name = name
        self._chain = []
        self._triggered = asyncio.Event()
        self._loop = None


_ct.CancelToken = _PatchedCancelToken

from quarkchain.p2p.nat import UPnPService

# QKCLogger.findCaller is missing the stacklevel param added in py3.8+,
# use a standard logger to avoid the incompatibility
import logging

Logger = __import__("quarkchain.utils", fromlist=["Logger"]).Logger
if not Logger._qkc_logger:
    Logger.set_logging_level("warning")
Logger._qkc_logger = logging.getLogger("qkc.test")


def _make_mock_service(external_ip="203.0.113.5"):
    """Create a mock WANIPConnection service."""
    service = MagicMock()
    service.service_type = "urn:schemas-upnp-org:service:WANIPConnection:1"

    async def async_call_action(action_name, **kwargs):
        if action_name == "GetExternalIPAddress":
            return {"NewExternalIPAddress": external_ip}
        return {}

    service.async_call_action = AsyncMock(side_effect=async_call_action)
    return service


@patch("quarkchain.p2p.nat.socket")
def test_get_internal_ip(mock_socket):
    mock_sock = MagicMock()
    mock_sock.getsockname.return_value = ("192.168.1.100", 12345)
    mock_socket.AF_INET = socket.AF_INET
    mock_socket.SOCK_DGRAM = socket.SOCK_DGRAM
    mock_socket.socket.return_value = mock_sock

    service = UPnPService(port=30303)
    ip = service._get_internal_ip()

    assert ip == "192.168.1.100"
    mock_sock.connect.assert_called_once_with(("8.8.8.8", 80))
    mock_sock.close.assert_called_once()


@pytest.mark.asyncio
async def test_get_external_ip_with_service():
    service = UPnPService(port=30303)
    service._service = _make_mock_service("203.0.113.5")

    ip = await service._get_external_ip()
    assert ip == "203.0.113.5"


@pytest.mark.asyncio
async def test_get_external_ip_no_service():
    service = UPnPService(port=30303)
    service._service = None

    ip = await service._get_external_ip()
    assert ip is None


@patch("quarkchain.p2p.nat.socket")
@pytest.mark.asyncio
async def test_add_port_mapping(mock_socket):
    mock_sock = MagicMock()
    mock_sock.getsockname.return_value = ("192.168.1.100", 12345)
    mock_socket.AF_INET = socket.AF_INET
    mock_socket.SOCK_DGRAM = socket.SOCK_DGRAM
    mock_socket.socket.return_value = mock_sock

    service = UPnPService(port=30303)
    mock_svc = _make_mock_service()
    service._service = mock_svc

    await service._add_port_mapping()

    assert mock_svc.async_call_action.call_count == 2
    calls = mock_svc.async_call_action.call_args_list
    assert calls[0].args[0] == "AddPortMapping"
    assert calls[0].kwargs["NewProtocol"] == "TCP"
    assert calls[0].kwargs["NewInternalClient"] == "192.168.1.100"
    assert calls[0].kwargs["NewExternalPort"] == 30303
    assert calls[1].args[0] == "AddPortMapping"
    assert calls[1].kwargs["NewProtocol"] == "UDP"


@patch("quarkchain.p2p.nat.async_search")
@patch("quarkchain.p2p.nat.socket")
@pytest.mark.asyncio
async def test_discover_success(mock_socket, mock_async_search):
    mock_sock = MagicMock()
    mock_sock.getsockname.return_value = ("192.168.1.100", 12345)
    mock_socket.AF_INET = socket.AF_INET
    mock_socket.SOCK_DGRAM = socket.SOCK_DGRAM
    mock_socket.socket.return_value = mock_sock

    mock_wan_service = _make_mock_service("203.0.113.5")

    # Simulate async_search calling the callback with a response
    async def fake_search(on_response, timeout=30):
        response = MagicMock()
        response.location = "http://192.168.1.1:5000/rootDesc.xml"

        # Create a fake device with WANIPConn service
        device = MagicMock()
        device.services = {"WANIPConn1": mock_wan_service}
        mock_wan_service.service_type = "urn:schemas-upnp-org:service:WANIPConnection:1"

        with patch("quarkchain.p2p.nat.UpnpFactory") as MockFactory:
            factory_instance = MockFactory.return_value
            factory_instance.async_create_device = AsyncMock(return_value=device)
            # We need to call the actual _discover, so let's set up differently
        return

    # Simpler approach: directly set the service and test discover flow
    svc = UPnPService(port=30303)
    svc._discover = AsyncMock()
    svc._service = mock_wan_service
    svc._session = MagicMock()
    svc._session.close = AsyncMock()

    external_ip = await svc.discover()

    assert external_ip == "203.0.113.5"
    assert mock_wan_service.async_call_action.call_count == 3  # 2x AddPortMapping + 1x GetExternalIPAddress


@pytest.mark.asyncio
async def test_discover_no_device():
    svc = UPnPService(port=30303)
    svc._discover = AsyncMock()  # _service stays None
    svc._session = MagicMock()
    svc._session.close = AsyncMock()

    result = await svc.discover()

    assert result is None


@pytest.mark.asyncio
async def test_delete_port_mapping():
    svc = UPnPService(port=30303)
    mock_svc = _make_mock_service()
    svc._service = mock_svc

    await svc._delete_port_mapping()

    # Should call DeletePortMapping for TCP and UDP
    delete_calls = [
        c for c in mock_svc.async_call_action.call_args_list
        if c.args[0] == "DeletePortMapping"
    ]
    assert len(delete_calls) == 2
    assert delete_calls[0].kwargs["NewProtocol"] == "TCP"
    assert delete_calls[1].kwargs["NewProtocol"] == "UDP"


@pytest.mark.asyncio
async def test_stop():
    svc = UPnPService(port=30303)
    mock_svc = _make_mock_service()
    svc._service = mock_svc
    mock_session = MagicMock()
    mock_session.close = AsyncMock()
    svc._session = mock_session

    await svc.stop()

    # Verify port mappings deleted and session closed
    delete_calls = [
        c for c in mock_svc.async_call_action.call_args_list
        if c.args[0] == "DeletePortMapping"
    ]
    assert len(delete_calls) == 2
    mock_session.close.assert_awaited_once()
    assert svc._session is None
