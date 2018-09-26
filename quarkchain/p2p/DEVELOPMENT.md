# QuarkChain p2p module

## Status
- Kademlia implementation

## Guidelines
- loosely based on [py-evm (Trinity)'s implementation of devp2p](https://github.com/ethereum/py-evm/tree/master/p2p)
- loosely follow [devp2p specifications](https://github.com/ethereum/devp2p)
- adopt [discv5](https://github.com/fjl/p2p-drafts)

## Motivation
First version of QuarkChain testnet used simple_network which connects all peers with a fully connected topology, and it soon became unrealistic to run a testnet with 100 nodes with fully connection (each node maintains 99 peers). We used pydevp2p that was part of pyethereum for QuarkChain testnet 1.0 which worked fine. However, pydevp2p is no longer in the list of [devp2p official implementations](https://github.com/ethereum/devp2p), albeit there is no real protocol changes of devp2p since 2014. And we need the new features brought by discv5. Discarding gevent and adopting asyncio is a bonus.

We need a better p2p library for QuarkChain mainnet.

We now propose our own implementation of devp2p-like specifications, learning from the implementations of ethereum (py-evm and go-ethereum) and adding our own features (treating shards as topics for discv5)
