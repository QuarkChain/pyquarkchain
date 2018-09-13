# QuarkChain

[![CircleCI](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master.svg?style=shield&circle-token=c17a071129e4ab6c0911154c955efc236b1a5015)](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master)

## Development Setup

QuarkChain should be run using [pypy](http://pypy.org/index.html) for better performance.

To install pypy3 on OSX, first install [Homebrew](https://brew.sh/) and then

```bash
brew install pypy3
```

It's is highly recommended to use [virtual environment](https://docs.python.org/3/library/venv.html) creating an isolated python environment for your project so that the python modules installed later will only affect this environment.

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
