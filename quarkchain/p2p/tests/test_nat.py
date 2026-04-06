import asyncio
import socket
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from quarkchain.p2p.cancel_token.token import OperationCancelled

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

from quarkchain.utils import Logger
if not Logger._qkc_logger:
    Logger.set_logging_level("warning")


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

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


@pytest.fixture
def mock_socket():
    with patch("quarkchain.p2p.nat.socket") as m:
        sock = MagicMock()
        sock.getsockname.return_value = ("192.168.1.100", 12345)
        m.AF_INET = socket.AF_INET
        m.SOCK_DGRAM = socket.SOCK_DGRAM
        m.socket.return_value = sock
        yield m, sock


@pytest.fixture
def mock_aiohttp():
    with patch("quarkchain.p2p.nat.aiohttp") as m:
        session = MagicMock()
        session.close = AsyncMock()
        m.ClientSession.return_value = session
        yield m, session


def _fake_wait_after(svc, iterations):
    """Return a fake wait that cancels after N iterations."""
    call_count = 0

    async def fake_wait(awaitable, timeout=None):
        nonlocal call_count
        await awaitable
        call_count += 1
        if call_count >= iterations:
            svc.cancel_token.trigger()
            raise OperationCancelled("test done")

    return fake_wait


# ---------------------------------------------------------------------------
# _get_internal_ip
# ---------------------------------------------------------------------------

def test_get_internal_ip(mock_socket):
    _, sock = mock_socket
    svc = UPnPService(port=30303)

    assert svc._get_internal_ip() == "192.168.1.100"
    sock.connect.assert_called_once_with(("8.8.8.8", 80))
    sock.close.assert_called_once()


# ---------------------------------------------------------------------------
# _get_external_ip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_external_ip_with_service():
    svc = UPnPService(port=30303)
    svc._service = _make_mock_service("203.0.113.5")

    assert await svc._get_external_ip() == "203.0.113.5"


@pytest.mark.asyncio
async def test_get_external_ip_no_service():
    svc = UPnPService(port=30303)
    svc._service = None

    assert await svc._get_external_ip() is None


@pytest.mark.asyncio
async def test_get_external_ip_exception_returns_none():
    svc = UPnPService(port=30303)
    svc._service = MagicMock()
    svc._service.async_call_action = AsyncMock(side_effect=RuntimeError("timeout"))

    assert await svc._get_external_ip() is None


# ---------------------------------------------------------------------------
# _add_port_mapping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_port_mapping(mock_socket):
    svc = UPnPService(port=30303)
    mock_svc = _make_mock_service()
    svc._service = mock_svc

    await svc._add_port_mapping()

    assert mock_svc.async_call_action.call_count == 2
    calls = mock_svc.async_call_action.call_args_list
    assert calls[0].args[0] == "AddPortMapping"
    assert calls[0].kwargs["NewProtocol"] == "TCP"
    assert calls[0].kwargs["NewInternalClient"] == "192.168.1.100"
    assert calls[0].kwargs["NewExternalPort"] == 30303
    assert calls[1].args[0] == "AddPortMapping"
    assert calls[1].kwargs["NewProtocol"] == "UDP"


# ---------------------------------------------------------------------------
# _delete_port_mapping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_port_mapping():
    svc = UPnPService(port=30303)
    mock_svc = _make_mock_service()
    svc._service = mock_svc

    await svc._delete_port_mapping()

    delete_calls = [
        c for c in mock_svc.async_call_action.call_args_list
        if c.args[0] == "DeletePortMapping"
    ]
    assert len(delete_calls) == 2
    assert delete_calls[0].kwargs["NewProtocol"] == "TCP"
    assert delete_calls[1].kwargs["NewProtocol"] == "UDP"


@pytest.mark.asyncio
async def test_delete_port_mapping_no_service():
    svc = UPnPService(port=30303)
    svc._service = None
    # Should not raise
    await svc._delete_port_mapping()


# ---------------------------------------------------------------------------
# _close_session
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_close_session():
    svc = UPnPService(port=30303)
    mock_session = MagicMock()
    mock_session.close = AsyncMock()
    svc._session = mock_session

    await svc._close_session()

    mock_session.close.assert_awaited_once()
    assert svc._session is None


@pytest.mark.asyncio
async def test_close_session_already_none():
    svc = UPnPService(port=30303)
    svc._session = None
    # Should not raise
    await svc._close_session()


# ---------------------------------------------------------------------------
# discover (end-to-end with mocked deps)
# ---------------------------------------------------------------------------

@patch("quarkchain.p2p.nat.UpnpFactory")
@patch("quarkchain.p2p.nat.AiohttpSessionRequester")
@patch("quarkchain.p2p.nat.async_search")
@pytest.mark.asyncio
async def test_discover_success(mock_async_search, mock_requester_cls,
                                mock_factory_cls, mock_socket, mock_aiohttp):
    _, session = mock_aiohttp
    mock_wan_service = _make_mock_service("203.0.113.5")

    fake_device = MagicMock()
    fake_device.services = {"WANIPConn1": mock_wan_service}

    mock_factory = mock_factory_cls.return_value
    mock_factory.async_create_device = AsyncMock(return_value=fake_device)

    async def fake_search(on_response, timeout=30):
        response = MagicMock()
        response.location = "http://192.168.1.1:5000/rootDesc.xml"
        await on_response(response)

    mock_async_search.side_effect = fake_search

    svc = UPnPService(port=30303)
    external_ip = await svc.discover()

    assert external_ip == "203.0.113.5"
    mock_factory.async_create_device.assert_awaited_once_with(
        "http://192.168.1.1:5000/rootDesc.xml"
    )
    # 2x AddPortMapping (TCP+UDP) + 1x GetExternalIPAddress
    assert mock_wan_service.async_call_action.call_count == 3


@patch("quarkchain.p2p.nat.async_search")
@pytest.mark.asyncio
async def test_discover_no_device(mock_async_search, mock_aiohttp):
    _, session = mock_aiohttp
    mock_async_search.side_effect = AsyncMock()

    svc = UPnPService(port=30303)
    result = await svc.discover()

    assert result is None
    session.close.assert_awaited_once()


@patch("quarkchain.p2p.nat.UpnpFactory")
@patch("quarkchain.p2p.nat.AiohttpSessionRequester")
@patch("quarkchain.p2p.nat.async_search")
@pytest.mark.asyncio
async def test_discover_skips_device_without_wanipconn(mock_async_search,
                                                       mock_requester_cls,
                                                       mock_factory_cls,
                                                       mock_aiohttp):
    _, session = mock_aiohttp
    # Device has no WANIPConn service
    fake_device = MagicMock()
    non_wan_service = MagicMock()
    non_wan_service.service_type = "urn:schemas-upnp-org:service:Layer3Forwarding:1"
    fake_device.services = {"L3Fwd": non_wan_service}

    mock_factory = mock_factory_cls.return_value
    mock_factory.async_create_device = AsyncMock(return_value=fake_device)

    async def fake_search(on_response, timeout=30):
        response = MagicMock()
        response.location = "http://192.168.1.1:5000/rootDesc.xml"
        await on_response(response)

    mock_async_search.side_effect = fake_search

    svc = UPnPService(port=30303)
    result = await svc.discover()

    assert result is None
    session.close.assert_awaited_once()


@patch("quarkchain.p2p.nat.UpnpFactory")
@patch("quarkchain.p2p.nat.AiohttpSessionRequester")
@patch("quarkchain.p2p.nat.async_search")
@pytest.mark.asyncio
async def test_discover_ignores_device_creation_error(mock_async_search,
                                                      mock_requester_cls,
                                                      mock_factory_cls,
                                                      mock_aiohttp):
    _, session = mock_aiohttp
    mock_factory = mock_factory_cls.return_value
    mock_factory.async_create_device = AsyncMock(
        side_effect=RuntimeError("connection refused")
    )

    async def fake_search(on_response, timeout=30):
        response = MagicMock()
        response.location = "http://192.168.1.1:5000/rootDesc.xml"
        await on_response(response)

    mock_async_search.side_effect = fake_search

    svc = UPnPService(port=30303)
    result = await svc.discover()

    assert result is None
    session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop():
    svc = UPnPService(port=30303)
    mock_svc = _make_mock_service()
    svc._service = mock_svc
    mock_session = MagicMock()
    mock_session.close = AsyncMock()
    svc._session = mock_session

    await svc.stop()

    delete_calls = [
        c for c in mock_svc.async_call_action.call_args_list
        if c.args[0] == "DeletePortMapping"
    ]
    assert len(delete_calls) == 2
    mock_session.close.assert_awaited_once()
    assert svc._session is None


# ---------------------------------------------------------------------------
# _run
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_refreshes_port_mapping(mock_socket):
    svc = UPnPService(port=30303)
    svc._service = _make_mock_service()
    svc._nat_portmap_lifetime = 0
    svc.events.started.set()
    svc.wait = _fake_wait_after(svc, iterations=2)

    await svc._run()

    add_calls = [
        c for c in svc._service.async_call_action.call_args_list
        if c.args[0] == "AddPortMapping"
    ]
    # TCP + UDP from one refresh (second iteration cancels before _add_port_mapping)
    assert len(add_calls) == 2


@pytest.mark.asyncio
async def test_run_skips_mapping_without_service():
    svc = UPnPService(port=30303)
    svc._service = None
    svc._nat_portmap_lifetime = 0
    svc.events.started.set()
    svc.wait = _fake_wait_after(svc, iterations=1)
    svc._add_port_mapping = AsyncMock()

    await svc._run()

    svc._add_port_mapping.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_exits_on_cancel():
    svc = UPnPService(port=30303)
    svc._service = _make_mock_service()
    svc._nat_portmap_lifetime = 0
    svc.events.started.set()

    async def fake_wait(awaitable, timeout=None):
        awaitable.close()  # prevent "coroutine never awaited" warning
        raise OperationCancelled("cancelled")

    svc.wait = fake_wait

    await svc._run()

    assert svc._service.async_call_action.call_count == 0


@pytest.mark.asyncio
async def test_run_continues_on_exception(mock_socket):
    svc = UPnPService(port=30303)
    svc._service = _make_mock_service()
    svc._nat_portmap_lifetime = 0
    svc.events.started.set()
    svc.wait = _fake_wait_after(svc, iterations=3)

    original_add = svc._add_port_mapping
    attempt = 0

    async def flaky_add():
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            raise RuntimeError("temporary failure")
        await original_add()

    svc._add_port_mapping = flaky_add

    await svc._run()

    assert attempt == 2
