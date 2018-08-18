# QuarkChain

[![CircleCI](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master.svg?style=shield&circle-token=c17a071129e4ab6c0911154c955efc236b1a5015)](https://circleci.com/gh/QuarkChain/pyquarkchain/tree/master)

## Development Setup

QuarkChain is configured to be run with [pypy](http://pypy.org/index.html) for better performance.

To install pypy3 on Mac, first install [Homebrew](https://brew.sh/) and then ``` brew install pypy3```

It's is highly recommended to use [virtual environment](https://docs.python.org/3/library/venv.html) creating an isolated python environment for your project.
The packages installed later will only affect this environment.

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
To install the required packages for the project. Under pyquarkchain dir where setup.py is located
```bash
pip install -e .
```
You might see failure when installing plyvel which is a python interface for leveldb.
The following commands should address the issue.
```bash
brew install leveldb
CFLAGS='-mmacosx-version-min=10.7 -stdlib=libc++' pip install plyvel
```
If you see an error complaining missing ```'openssl/aes.h'```, export the following flags and try again.
```bash
export LDFLAGS="-L/usr/local/opt/openssl/lib"
export CPPFLAGS="-I/usr/local/opt/openssl/include"
```

Start running a cluster
```bash
cd quarkchain/cluster
pypy3 cluster.py --mine=True
```

Running with P2P network: 1. bootstrap node; 2. another node
```
pypy3 cluster.py --devp2p=True (--mine=True)
pypy3 cluster.py --devp2p=True --db_prefix=./dbx --port_start=39000 --p2p_port=39291 --json_rpc_port=39391 --devp2p_port=29001
```

Running >1 clusters with P2P network: (--mine=True sets exactly 1 cluster to mine)
```
pypy3 multi_cluster.py --num_cluster=10 (--mine=True)
```

## Development Flow

[`pre-commit`](https://pre-commit.com) is used to manage git hooks.

```
# make sure `pre-commit` is installed (homebrew, pip, etc)
pre-commit install
```

Currently `black` is used to format modified python code. Refer to STYLE for coding style suggestions.

## Docker Deployment
####TODO: update this section with pypy command
First [install docker](https://docs.docker.com/docker-for-mac/install/)

Build docker image
```
docker build -t quarkchain .
```


## Running the webserver/explorer (local development)
1. [Install Docker](https://docs.docker.com/docker-for-mac/install/)

2. Build the Dockerfile into an image. Run the following in the root of the repository:
```
docker build -t quarkchain .
```

3. Run the Docker image that was just built as a container:
```
docker run -d -P --name quarkchain -p 8000:80 -e ENVIRONMENT=development -v /PATH/TO/YOUR/pyquarkchain/root/:/code quarkchain
```

4. If you need to SSH into the container, you will need to add your SSH public key to ./dockers/webserver/ssh/authorized_keys. Then follow steps 2 and 3 again. You'll need to run:
```
docker port quarkchain
```
in order to find the port mapping for port 22. Then SSH using:
```
ssh -p <CONTAINER_SSH_PORT> root@localhost
```

5. To stop the container:
```
docker stop quarkchain
```

6. To remove the container:
```
docker rm quarkchain
```

p7. You can run supervisor commands on the container without having to SSH, like so:
```
docker exec quarkchain supervisorctl restart uwsgi
docker exec quarkchain supervisorctl restart all
```
