# QuarkChain

[![CircleCI](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master.svg?style=shield&circle-token=c17a071129e4ab6c0911154c955efc236b1a5015)](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master)

QuarkChain is a sharded blockchain protocol that employs a two-layer architecture - one extensible sharding layer consisting of multiple shard chains processing transactions and one root chain layer securing the network and coordinating cross-shard transactions among shard chains. The capacity of the network scales linearly as the number of shard chains increase while the root chain is always providing strong security guarantee regardless of the number of shards. QuarkChain testnet consistently hit [10,000+ TPS](https://youtu.be/dUldrq3zKwE?t=8m28s) with 256 shards run by 50 clusters consisting of 6450 servers with each loadtest submitting 3,000,000 transactions to the network.

## Features

- Cluster implementation allowing multiple processes / physical machines to work together as a single full node
- State sharding dividing global state onto independent processing and storage units allowing the network capacity to scale linearly by adding more shards
- Cross-shard transaction allowing native token transfers among shard chains
- Adding shards dynamically to the network
- Support of different mining algorithms on different shards
- P2P network allowing clusters to join and leave anytime
- Fully compatible with Ethereum smart contract

## Development Setup

QuarkChain should be run using [pypy](http://pypy.org/index.html) for better performance.

To install pypy3 on OSX, first install [Homebrew](https://brew.sh/)

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Then install pypy3 and other dependencies

```bash
brew install pypy3 gmp pkg-config
```

It's is highly recommended to use [virtual environment](https://docs.python.org/3/library/venv.html) creating an isolated python environment for your project so that the python modules installed later will only affect this environment.

To clone the code to your target directory

```bash
git clone git@github.com:QuarkChain/pyquarkchain.git
cd pyquarkchain
```

To create a virtual environment

```bash
mkdir ~/virtualenv
pypy3 -m venv ~/virtualenv/qc
```

As the virtual env is created with pypy3 once the env is activated all the python and pip commands will point to their pypy3 versions automatically.

To activate the virtual environment

```bash
source ~/virtualenv/qc/bin/activate
```

Install rocksdb which is required by the `python-rocksdb` module in the next step

```bash
brew install rocksdb
```

To install the required modules for the project. Under `pyquarkchain` dir where `setup.py` is located

```bash
pip install -e .
```

Once all the modules are installed, try running all the unit tests under `pyquarkchain`

```
pypy3 -m unittest
```

## Development Flow

[`pre-commit`](https://pre-commit.com) is used to manage git hooks.

```
pip install pre-commit
pre-commit install
```

[black](https://github.com/ambv/black) is used to format modified python code, which will be automatically triggered on new commit after running the above commands. Refer to [STYLE](https://github.com/QuarkChain/pyquarkchain/blob/master/STYLE) for coding style suggestions.

## Run Cluster

Start running a cluster. The default cluster has 8 shards and 4 slaves.

```bash
cd quarkchain/cluster
pypy3 cluster.py --mine
```

Run multiple clusters with P2P network (--mine sets exactly 1 cluster to mine)

```bash
pypy3 multi_cluster.py --num_clusters=3 --devp2p_enable
```

## Verify cluster running

The default port number we setup for cluster is 38391. To verify the cluster is ruunig, you can run:

```bash
curl -X POST --data '{
    "jsonrpc": "2.0",
    "method": "networkInfo",
    "id": 1
}' localhost:38391 | jq .
```

An example of output:

```bash
{
  "jsonrpc": "2.0",
  "result": {
    "networkId": "0x3",
    "shardSize": "0x8",
    "syncing": false,
    "mining": true,
    "shardServerCount": 4
  },
  "id": 1
}
```

For details, check our [developer documentation](https://developers.quarkchain.io).


## Issue
Please open issues on github to report bugs or make feature requests.

## Contribution
All the help from community is appreciated! If you are interested in working on features or fixing bugs, please open an issue first
to describe the task you are planning to do. For small fixes (a few lines of change) feel
free to open pull requests directly.

## Developer Community
Join our developer community on [Discord](https://discord.gg/Jbp35ZC).
