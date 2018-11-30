# QuarkChain

[![CircleCI](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master.svg?style=shield&circle-token=c17a071129e4ab6c0911154c955efc236b1a5015)](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master)

QuarkChain is a sharded blockchain protocol that employs a two-layer architecture - one extensible sharding layer consisting of multiple shard chains processing transactions and one root chain layer securing the network and coordinating cross-shard transactions among shard chains. The capacity of the network scales linearly as the number of shard chains increase while the root chain is always providing strong security guarantee regardless of the number of shards. QuarkChain testnet consistently hit [10,000+ TPS](https://youtu.be/dUldrq3zKwE?t=8m28s) with 256 shards run by 50 clusters consisting of 6450 servers with each loadtest submitting 3,000,000 transactions to the network.

## Features

- Cluster implementation allowing multiple processes / physical machines to work together as a single full node
- State sharding dividing global state onto independent processing and storage units allowing the network capacity to scale linearly by adding more shards
- Cross-shard transaction allowing native token transfers among shard chains
- Adding shards dynamically to the network
- Support of different mining algorithms on different shards
- P2P network allowing clusters to join and leave anytime with encrypted transport
- Fully compatible with Ethereum smart contract

## Design

![QuarkChain Cluster](https://docs.google.com/drawings/d/e/2PACX-1vRkF6Wd-I-1j-601IFWPwd9u8A5oqa_c2JVBad1SDY48ATY1aRaJvhObiX8p9Jh1ra5G-HIqhhYl0NM/pub?w=960&h=576)

Check out the [Wiki](https://github.com/QuarkChain/pyquarkchain/wiki) to understand the design of QuarkChain.

## Development Setup

QuarkChain should be run using [pypy](http://pypy.org/index.html) for better performance. *The rest of section uses **OSX** as the reference for environment set-up.*

To install pypy3 on OSX, first install [Homebrew](https://brew.sh/)

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Then install pypy3 and other dependencies

```bash
brew install pypy3 gmp pkg-config openssl
```

It's is highly recommended to use [virtual environment](https://docs.python.org/3/library/venv.html) creating an isolated python environment for your project so that the python modules installed later will only affect this environment.

To clone the code to your target directory

```bash
git clone https://github.com/QuarkChain/pyquarkchain.git
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
# you may want to set the following if cryptography complains about header files: (https://github.com/pyca/cryptography/issues/3489)
# export CPPFLAGS=-I/usr/local/opt/openssl/include
# export LDFLAGS=-L/usr/local/opt/openssl/lib
pip install -e .
```

Once all the modules are installed, try running all the unit tests under `pyquarkchain`

```
python -m pytest quarkchain
```

## Development Flow

[`pre-commit`](https://pre-commit.com) is used to manage git hooks.

```
pip install pre-commit
pre-commit install
```

[black](https://github.com/ambv/black) is used to format modified python code, which will be automatically triggered on new commit after running the above commands. Refer to [STYLE](https://github.com/QuarkChain/pyquarkchain/blob/master/STYLE) for coding style suggestions.

## Cluster Launch

### Run a private cluster on the QuarkChain testnet 2.0
If you are on a private network (e.g. running QuarkChain clusters from a laptop which connects to a router), you need to first setup [port forwarding](https://github.com/QuarkChain/pyquarkchain/wiki/Private-Network-Setting%2C-Port-Forwarding) for UDP/TCP 38291.

Then fill in your own coinbase address and [bootstrap a cluster](https://github.com/QuarkChain/pyquarkchain/wiki/Run-a-Private-Cluster-on-the-QuarkChain-Testnet-2.0) on QuarkChain Testnet 2.0.

We provide the [demo implementation of CPU mining software](https://github.com/QuarkChain/pyquarkchain/wiki/Demo-Implementation-of-CPU-Mining). Please refer to [QuarkChain mining](https://github.com/QuarkChain/pyquarkchain/wiki/Introduction-of-Mining-Algorithms) for more details.

### Run a single cluster
Start running a cluster. The default cluster has 8 shards and 4 slaves.

```bash
cd quarkchain/cluster
pypy3 cluster.py
```

### Run multiple clusters
Run multiple clusters with P2P network on a single machine:
```bash
# This part has been deprecated, please file an issue if you need this feature
# pypy3 multi_cluster.py --num_clusters=3 --devp2p_enable
```

### Run multiple clusters with P2P network on different machines.
Just follow the same command to run single cluster and provide `--bootnodes` flag to discover and connect to other clusters. Make sure ports are open and accessible from outside world: this means if you are running on AWS, open the ports (default both UDP and TCP 38291) in security group; if you are running from a LAN (connecting to the internet through a router), you need to setup port forwarding for UDP/TCP 38291. We have a convenience UPNP module as well, but you will need to check if it has successfully set port forwarding.

(Optional) Not needed if you are joining a testnet or mainnet. If you are starting your own network, first start the bootstrap cluster:
```bash
# optional, run python quarkchain/tools/newkey.py and note $BOOTSTRAP_PRIV_KEY and $BOOTSTRAP_PUB_KEY
pypy3 cluster.py --p2p --privkey=$BOOTSTRAP_PRIV_KEY
```

Then start other clusters and provide the bootnode.
```bash
BOOTSTRAP_NODE=enode://$BOOTSTRAP_PUB_KEY@$BOOTSTRAP_IP:$BOOTSTRAP_DISCOVERY_PORT
pypy3 cluster.py --p2p --bootnodes=$BOOTSTRAP_ENODE
```

Effectively, `newkey.py` gives the bootstrap node an identity, and you will need to provide the public key to anyone who wants to connect to the bootnodes for discovery. Read https://github.com/QuarkChain/pyquarkchain/wiki/Networking#commandline-flags-explained for details on the commandline flags.
```

Effectively, `newkey.py` gives the bootstrap node an identity, and you will need to provide the public key to anyone who wants to connect to the bootnodes for discovery. Read https://github.com/QuarkChain/pyquarkchain/wiki/Networking#commandline-flags-explained for details on the commandline flags.

## Monitor Cluster
Use the [`stats`](https://github.com/QuarkChain/pyquarkchain/blob/master/quarkchain/tools/stats) tool in the repo to monitor the status of a cluster. It queries the given cluster through JSON RPC every 10 seconds and produces an entry.
```bash
$ quarkchain/tools/stats --ip=localhost
----------------------------------------------------------------------------------------------------
                                      QuarkChain Cluster Stats
----------------------------------------------------------------------------------------------------
CPU:                8
Memory:             16 GB
IP:                 localhost
Shards:             8
Servers:            4
Shard Interval:     60
Root Interval:      10
Syncing:            False
Mining:             False
Peers:              127.0.0.1:38293, 127.0.0.1:38292
----------------------------------------------------------------------------------------------------
Timestamp                     TPS   Pending tx  Confirmed tx       BPS      SBPS      ROOT       CPU
----------------------------------------------------------------------------------------------------
2018-09-21 16:35:07          0.00            0             0      0.00      0.00        84     12.50
2018-09-21 16:35:17          0.00            0          9000      0.02      0.00        84      7.80
2018-09-21 16:35:27          0.00            0         18000      0.07      0.00        84      6.90
2018-09-21 16:35:37          0.00            0         18000      0.07      0.00        84      4.49
2018-09-21 16:35:47          0.00            0         18000      0.10      0.00        84      6.10
```

## JSON RPC
JSON RPCs are defined in [`jsonrpc.py`](https://github.com/QuarkChain/pyquarkchain/blob/master/quarkchain/cluster/jsonrpc.py). Note that there are two JSON RPC ports. By default they are 38491 for private RPCs and 38391 for public RPCs. Since you are running your own clusters you get access to both.

Public RPCs are documented in the [Developer Guide](https://developers.quarkchain.io/#json-rpc). You can use the client library [quarkchain-web3.js](https://github.com/QuarkChain/quarkchain-web3.js) to query account state, send transactions, deploy and call smart contracts. Here is [a simple example](https://gist.github.com/qcgg/1ab0352c5b2299270b5795648cca83d8) to deploy smart contract on QuarkChain using the client library.

You may find a list of accounts with tokens preallocated through the genesis blocks [here](https://github.com/QuarkChain/pyquarkchain/blob/master/quarkchain/genesis_data/alloc.json). Feel free to use any of them to issue transactions.

## Loadtest
Follow [this wiki page](https://github.com/QuarkChain/pyquarkchain/wiki/Loadtest) to loadtest your cluster and see how fast it processes large volumn of transacations.

## Issue
Please open issues on github to report bugs or make feature requests.

## Contribution
All the help from community is appreciated! If you are interested in working on features or fixing bugs, please open an issue first
to describe the task you are planning to do. For small fixes (a few lines of change) feel
free to open pull requests directly.

## Developer Community
Join our developer community on [Discord](https://discord.gg/Jbp35ZC).
