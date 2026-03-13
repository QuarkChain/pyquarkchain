
import aiohttp
import asyncio
import socket
from contextlib import suppress
from typing import Optional

from quarkchain.p2p.cancel_token.token import CancelToken, OperationCancelled
from quarkchain.p2p.service import BaseService

from async_upnp_client.aiohttp import AiohttpSessionRequester
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
        
        self._service = None
        self._refresh_task = None
        self._running = False


    # -----------------------------
    # Public API
    # -----------------------------

    async def discover(self) -> Optional[str]:
        """
        Discover router and create initial port mapping.
        Returns external IP if successful.
        """
        session = aiohttp.ClientSession()
        try:
            await self._discover(session)
        finally:
            await session.close()

        if not self._service:
            self.logger.warning("No UPnP WANIP service found")
            return None

        await self._add_port_mapping()

        return await self._get_external_ip()
		

    async def stop(self):
        self._running = False
        if self._refresh_task:
            self._refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._refresh_task

        await self._delete_port_mapping()


        
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
                await self._add_port_mapping()
            except OperationCancelled:
                break
            except Exception:
                self.logger.exception("Failed to setup NAT portmap")


    async def _discover(self, session):
        found = asyncio.Event()

        requester = AiohttpSessionRequester(session)
        factory = UpnpFactory(requester)

        async def on_response(response):
            try:
                device = await factory.async_create_device(response.location)

                for service in device.services.values():
                    if "WANIPConn" in service.service_type:
                        self._service = service
                        self.logger.info("Found UPnP WANIP service")
                        found.set()
                        return
            except Exception as e:
                self.logger.debug(f"Ignoring device: {e}")

        await async_search(on_response)

        try:
            await asyncio.wait_for(found.wait(), timeout=UPNP_DISCOVER_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            self.logger.warning("No suitable UPnP device discovered")


    async def _add_port_mapping(self):
        internal_ip = self._get_internal_ip()

        self.logger.info(
            f"Adding port mapping {self.external_port}->{internal_ip}:{self.internal_port}"
        )

        for protocol, description in [
            ("TCP", "ethereum p2p"),
            ("UDP", "ethereum discovery"),
        ]:
            await self._service.async_call_action(
                "AddPortMapping",
                NewRemoteHost="", # should we use _get_external_ip() to replace this?
                NewExternalPort=self.port,
                NewProtocol=protocol,
                NewInternalPort=self.port,
                NewInternalClient=internal_ip,
                NewEnabled=1,
                NewPortMappingDescription=description,
                NewLeaseDuration=self._nat_portmap_lifetime,
            )

    async def _delete_port_mapping(self):
        if not self._service:
            return

        with suppress(Exception):
            await self._service.async_call_action(
                "DeletePortMapping",
                NewRemoteHost="",
                NewExternalPort=self.external_port,
                NewProtocol=self.protocol,
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


    def _get_internal_ip(self) -> str:
        """
        Robust internal IP detection using socket trick.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()	
