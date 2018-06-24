# docker pull qcgg/circleci-pypy3-rocksdb
FROM ubuntu:xenial

MAINTAINER qcgg

WORKDIR /opt

# set up basic system packages
RUN apt-get update
RUN apt-get install -y git build-essential wget pkg-config libgmp-dev libssl-dev ca-certificates

# install rocksdb
RUN apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libzstd-dev
RUN git clone https://github.com/facebook/rocksdb.git
RUN cd rocksdb && DEBUG_LEVEL=0 make shared_lib install-shared
RUN cp /usr/local/lib/librocksdb.* /usr/lib/

# intall pypy3
RUN wget https://bitbucket.org/pypy/pypy/downloads/pypy3-v6.0.0-linux64.tar.bz2
RUN tar fxv pypy3-v6.0.0-linux64.tar.bz2
ENV PATH="${PATH}:/opt/pypy3-v6.0.0-linux64/bin/"
RUN pypy3 -m ensurepip
RUN pypy3 -m pip install --upgrade pip wheel

WORKDIR /