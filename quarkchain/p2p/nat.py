import aiohttp
import asyncio
import socket
from contextlib import suppress
from typing import Optional
from urllib.parse import urlparse

from quarkchain.p2p.cancel_token.token import CancelToken, OperationCancelled
from quarkchain.p2p.service import BaseService

from async_upnp_client.aiohttp import AiohttpSessionRequester
from async_upnp_client.exceptions import UpnpActionResponseError
from async_upnp_client.client_factory import UpnpFactory
from async_upnp_client.search import async_search

# UPnP discovery can take a long time, so use a loooong timeout here.
UPNP_DISCOVER_TIMEOUT_SECONDS = 30


class UPnPService(BaseService):
    """
    Generate a mapping of external network IP address/port to internal IP address/port,
    using the Universal Plug 'n' Play standard.
    """

    _nat_portmap_lifetime = 30 * 60

    def __init__(self, port: int, token: CancelToken = None) -> None:
        """
        :param port: The port that a server wants to bind to on this machine, and
        make publicly accessible.
        """
        super().__init__(token)
        self.port = port
        self._session = None
        self._service = None
        self._router_host = None
        self._discover_lock = asyncio.Lock()


    # -----------------------------
    # Public API
    # -----------------------------

    async def discover(self) -> Optional[str]:
        """
        Discover router and create initial port mapping.
        Returns external IP if successful, or None if UPnP is unavailable or fails.

        Failures are best-effort: any exception is logged and None is returned so
        server startup can continue without UPnP.

        Concurrent calls are serialised by _discover_lock so that two callers cannot
        race on _session / _service state.
        """
        async with self._discover_lock:
            await self._close_session()
            self._service = None
            self._router_host = None
            self._session = aiohttp.ClientSession()
            try:
                await self._discover(self._session)

                if not self._service:
                    self.logger.warning("No UPnP WANIP service found")
                    return None

                await self._add_port_mapping()
                return await self._get_external_ip()
            except Exception:
                self.logger.exception("UPnP setup failed; continuing without NAT port mapping")
                self._service = None
                return None
            finally:
                # If setup failed (no service), the session is no longer needed.
                if not self._service:
                    await self._close_session()

    async def stop(self) -> None:
        await self._delete_port_mapping()
        await self._close_session()

    async def _cleanup(self) -> None:
        """Called by BaseService.cleanup() when the service shuts down."""
        await self.stop()

    async def _close_session(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None


    # -----------------------------
    # Internal logic
    # -----------------------------

    async def _run(self) -> None:
        """Run an infinite loop refreshing our NAT port mapping.

        On every iteration we configure the port mapping with a lifetime of 30 minutes and then
        sleep for that long as well.
        """
        while self.is_operational:
            try:
                # Wait for the port mapping lifetime, and then try registering it again
                await self.wait(asyncio.sleep(self._nat_portmap_lifetime))
                if self._service:
                    await self._add_port_mapping()
            except OperationCancelled:
                break
            except Exception:
                self.logger.exception("Failed to setup NAT portmap")

    async def _discover(self, session):
        requester = AiohttpSessionRequester(session)
        factory = UpnpFactory(requester)
        found = asyncio.Event()

        async def on_response(response):
            if self._service:
                return
            # async_upnp_client passes headers as a CaseInsensitiveDict
            location = response.get("location") if hasattr(response, "get") else getattr(response, "location", None)
            if not location:
                return
            try:
                device = await factory.async_create_device(location)
                # Re-check: concurrent callbacks may have found the service while we awaited
                if self._service:
                    return

                def _iter_services(dev):
                    yield from dev.services.values()
                    for sub in getattr(dev, "embedded_devices", {}).values():
                        yield from _iter_services(sub)

                for service in _iter_services(device):
                    if "WANIPConn" in service.service_type:
                        self._service = service
                        self._router_host = urlparse(location).hostname
                        self.logger.info("Found UPnP WANIP service")
                        found.set()
                        return
            except Exception as e:
                self.logger.debug(f"Ignoring device: {e}")

        search_task = asyncio.ensure_future(
            async_search(on_response, timeout=UPNP_DISCOVER_TIMEOUT_SECONDS)
        )
        try:
            await asyncio.wait_for(found.wait(), timeout=UPNP_DISCOVER_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            pass
        finally:
            search_task.cancel()
            with suppress(asyncio.CancelledError):
                await search_task

    async def _add_port_mapping(self) -> None:
        internal_ip = self._get_internal_ip()
        if not internal_ip:
            raise RuntimeError("Failed to find internal IP address for UPnP port mapping")

        self.logger.info(
            f"Adding port mapping {self.port}->{internal_ip}:{self.port}"
        )

        protocols_added = []
        try:
            for protocol, description in [
                ("TCP", "ethereum p2p"),
                ("UDP", "ethereum discovery"),
            ]:
                try:
                    await self._service.async_call_action(
                        "AddPortMapping",
                        NewRemoteHost="",
                        NewExternalPort=self.port,
                        NewProtocol=protocol,
                        NewInternalPort=self.port,
                        NewInternalClient=internal_ip,
                        NewEnabled=True,
                        NewPortMappingDescription=description,
                        NewLeaseDuration=self._nat_portmap_lifetime,
                    )
                    protocols_added.append(protocol)
                except UpnpActionResponseError as e:
                    if e.error_code == 718:
                        # ConflictInMappingEntry: an entry already exists (e.g. previous run
                        # didn't clean up). Treat as success — the mapping is in place.
                        self.logger.info(
                            "NAT %s port mapping already configured, not overriding it",
                            protocol,
                        )
                    else:
                        raise
        except Exception:
            # Roll back any mappings that succeeded before the failure.
            for protocol in protocols_added:
                with suppress(Exception):
                    await self._service.async_call_action(
                        "DeletePortMapping",
                        NewRemoteHost="",
                        NewExternalPort=self.port,
                        NewProtocol=protocol,
                    )
            raise

    async def _delete_port_mapping(self) -> None:
        if not self._service:
            return

        for protocol in ["TCP", "UDP"]:
            with suppress(Exception):
                await self._service.async_call_action(
                    "DeletePortMapping",
                    NewRemoteHost="",
                    NewExternalPort=self.port,
                    NewProtocol=protocol,
                )
        self.logger.info("Deleted UPnP port mapping")


    async def _get_external_ip(self) -> Optional[str]:
        if not self._service:
            return None

        try:
            result = await self._service.async_call_action("GetExternalIPAddress")
            return result.get("NewExternalIPAddress")
        except Exception as e:
            self.logger.warning(f"Failed to get external IP: {e}")
            return None


    def _get_internal_ip(self) -> Optional[str]:
        if not self._router_host:
            return None
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((self._router_host, 80))
            return s.getsockname()[0]
        except Exception as e:
            self.logger.warning(f"Failed to find internal IP: {e}")
            return None
        finally:
            s.close()


if __name__ == "__main__":
    import argparse

    from quarkchain.utils import Logger
    Logger.set_logging_level("info")

    parser = argparse.ArgumentParser(description="Test UPnP NAT port mapping")
    parser.add_argument("--port", type=int, default=38291, help="Port to map (default: 38291)")
    args = parser.parse_args()

    async def main():
        svc = UPnPService(port=args.port)

        # Test UPnP discover + port mapping
        print(f"\nDiscovering UPnP devices (timeout {UPNP_DISCOVER_TIMEOUT_SECONDS}s)...")
        external_ip = await svc.discover()
        if external_ip:
            internal_ip = svc._get_internal_ip()
            print(f"Internal IP: {internal_ip}")
            print(f"External IP: {external_ip}")
            print(f"Port {args.port} mapped successfully")
            input("Press Enter to remove mapping and exit...")
            await svc.stop()
            print("Mapping removed")
        else:
            print("UPnP discovery failed - no suitable device found")

    asyncio.run(main())
