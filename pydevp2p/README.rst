===============================
pydevp2p
===============================

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/ethereum/pydevp2p
   :target: https://gitter.im/ethereum/pydevp2p?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

.. image:: https://badge.fury.io/py/devp2p.png
    :target: https://badge.fury.io/py/devp2p

.. image:: https://travis-ci.org/ethereum/pydevp2p.png?branch=master
        :target: https://travis-ci.org/ethereum/pydevp2p

.. image:: https://coveralls.io/repos/ethereum/pydevp2p/badge.svg
        :target: https://coveralls.io/r/ethereum/pydevp2p

.. image:: https://readthedocs.org/projects/pydevp2p/badge/?version=latest
        :target: https://readthedocs.org/projects/pydevp2p/?badge=latest


Python implementation of the Ethereum P2P stack

* Free software: BSD license
* Documentation: https://pydevp2p.readthedocs.org.

Introduction
------------

pydevp2p is the Python implementation of the RLPx network layer.
RLPx provides a general-purpose transport and interface for applications to communicate via a p2p network. The first version is geared towards building a robust transport, well-formed network, and software interface in order to provide infrastructure which meets the requirements of distributed or decentralized applications such as Ethereum. Encryption is employed to provide better privacy and integrity than would be provided by a cleartext implementation.

RLPx underpins the DEVp2p interface:

* `https://github.com/ethereum/wiki/wiki/ÐΞVp2p-Wire-Protocol <https://github.com/ethereum/wiki/wiki/ÐΞVp2p-Wire-Protocol>`_
* `https://github.com/ethereum/wiki/wiki/libp2p-Whitepaper <https://github.com/ethereum/wiki/wiki/libp2p-Whitepaper>`_

Full spec:

* https://github.com/ethereum/devp2p/blob/master/rlpx.md

Dependencies
------------

On Ubuntu::

    $ sudo apt-get install libssl-dev build-essential automake pkg-config libtool libffi-dev libgmp-dev

Features
--------
* Node Discovery and Network Formation
* Peer Preference Strategies
* Peer Reputation
* Multiple protocols
* Encrypted handshake
* Encrypted transport
* Dynamically framed transport
* Fair queuing

Security Overview
-------------------
* nodes have access to a uniform network topology
* peers can uniformly connect to network
* network robustness >= kademlia
* protocols sharing a connection are provided uniform bandwidth
* authenticated connectivity
* authenticated discovery protocol
* encrypted transport (TCP now; UDP in future)
* robust node discovery
