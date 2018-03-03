# quarkchain

# Setup

## For development
```
python3 setup.py install
```
or
```
pip install requirements.txt
```

Running network
```
python3 -c "import quarkchain.simple_network; quarkchain.simple_network.main()"
```

## For Docker
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