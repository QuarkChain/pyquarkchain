"""
This is an exploratory piece of code to understand how UDP works with NAT.

Tested with an Apple AirPort Extreme Router:
1. run this file on a computer (ALICE) behind router
2. find a machine (BOB) with public IP (eg. AWS), run the following code snippet, ALICE would not be able to receive the UDP packets:
import socket
import time
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('0.0.0.0', 60999))
while True:
    sock.sendto(b"hello there", ("$PUBLIC_IP_OF_ALICE", 9999))
    time.sleep(0.2)
3. uncomment the part `transport.sendto(b"hello world", ("$PUBLIC_IP_OF_BOB", 60999))`, and run this file again

It looks our router determined that ALICE:9999 is communicating with BOB:60999, and setup NAT traversal automatically so that packets that come from BOB:60999 (note we set up sock.bind so that packets are marked from 60999 although nobody is listening on BOB) are automatically forwarded to ALICE:9999.

This demonstrates how NAT traversal is automatically handled in certain cases.

In real world usage, if you want a reliable way for your application to be connected from outside your private network, setup port forwarding on your router, as there is no guarantee that this will work in all cases.
"""

import asyncio


class EchoServerProtocol:
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode()
        print('Received %r from %s' % (message, addr))
        # print('Send %r to %s' % (message, addr))
        # self.transport.sendto(data, addr)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    port = 9999
    print("Starting UDP server on port {}".format(port))
    # One protocol instance will be created to serve all client requests
    listen = loop.create_datagram_endpoint(
        EchoServerProtocol, local_addr=('0.0.0.0', port))
    transport, protocol = loop.run_until_complete(listen)

    # transport.sendto(b"hello world", ("$PUBLIC_IP_OF_BOB", 60999))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    transport.close()
    loop.close()
