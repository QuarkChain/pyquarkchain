# QuarkChain

## Development Setup

(Mac OS X) [install python3](http://docs.python-guide.org/en/latest/starting/install3/osx/)

It's is highly recommended to use [virtual environment](https://docs.python.org/3/library/venv.html) creating an isolated python environment for your project.
The packages installed later will only affect this environment.

To create a virtual environment
```
mkdir ~/virtualenv
python3 -m venv ~/virtualenv/qc
```
As the virtual env is created with python3 once the env is activated all the python and pip commands will point to their python3 versions automatically.

To activate the virtual environment
```
source ~/virtualenv/qc/bin/activate
```
To install the required packages for the project. Under the same directory where setup.py is located.
```
pip install -e .
```

Running network
```
python simple_network.py
```

## Docker Deployment
First [install docker](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

Build docker image
```
docker build -t quarkchain .
```

Run network inside docker
```
docker run -it quarkchain /bin/bash
python3 -c "import quarkchain.simple_network; quarkchain.simple_network.main()"
```

Run network outside docker
```
docker run -t quarkchain python3 -c "import quarkchain.simple_network; quarkchain.simple_network.main()"
```

## Running with P2P Network

1. install pydevp2p, pyquarkchain
```
python setup.py install; cd pydevp2p; python setup.py install; cd ..
```
2. Run three or more pyquarkchain instance
```
python quarkchain/p2pnetwork.py --i_am_seed=true
python quarkchain/p2pnetwork.py --db_path=./db1 --enable_local_server=true --local_port=5858
python quarkchain/p2pnetwork.py --db_path=./db2
```
3. Run miner
```
python quarkchain/miner.py --local_port=5858
```


## Running the webserver/explorer (local development)
1. [Install Docker](https://docs.docker.com/docker-for-mac/install/)

2. Build the Dockerfile into an image. Run the following in the root of the repository:
```
docker build -t quarkchain .
```

3. Run the Docker image that was just built as a container:
```
docker run -d -P --name quarkchain -p 8000:80 -e ENVIRONMENT=development quarkchain
```

4. If you need to SSH into the container, you will need to add your SSH public key to ./dockers/webserver/ssh/authorized_keys. Then follow steps 2 and 3 again. You'll need to run:
```docker port quarkchain```

in order to find the port mapping for port 22. Then SSH using:

```ssh -p <CONTAINER_SSH_PORT> root@localhost```

5. To stop the container:
```docker stop quarkchain```

6. To remove the container:
```docker rm quarkchain```

7. You can run supervisor commands on the container without having to SSH, like so:
```
docker exec quarkchain supervisorctl restart uwsgi
docker exec quarkchain supervisorctl restart all
```



