# docker pull qcgg/circleci-pypy3-rocksdb
FROM ubuntu:xenial

MAINTAINER qcgg

WORKDIR /opt

# set up basic system packages
RUN apt-get update
# for libxml2 and libxslt1: https://stackoverflow.com/questions/5178416/libxml-install-error-using-pip
RUN apt-get install -y git build-essential wget pkg-config libgmp-dev libssl-dev ca-certificates python3-dev libxml2-dev libxslt1-dev

# install rocksdb
RUN apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libzstd-dev
RUN git clone https://github.com/facebook/rocksdb.git
RUN cd rocksdb && DEBUG_LEVEL=0 make shared_lib install-shared
RUN ln -fs /opt/rocksdb/librocksdb.so* /usr/lib/

# intall pypy3
RUN wget https://bitbucket.org/pypy/pypy/downloads/pypy3-v6.0.0-linux64.tar.bz2
RUN tar fxv pypy3-v6.0.0-linux64.tar.bz2
ENV PATH="${PATH}:/opt/pypy3-v6.0.0-linux64/bin/"
RUN pypy3 -m ensurepip
RUN pypy3 -m pip install --upgrade pip wheel

WORKDIR /
