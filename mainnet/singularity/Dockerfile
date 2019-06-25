FROM ubuntu:bionic

LABEL maintainer="quarkchain"

### set up basic system packages
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y libpq-dev libxml2-dev libxslt1-dev nginx openssh-client openssh-server openssl rsyslog rsyslog-gnutls liblcms2-dev libwebp-dev python-tk libfreetype6-dev vim-nox imagemagick libffi-dev libgmp-dev build-essential libssl-dev software-properties-common pkg-config libtool python3-dev git-core jq screen curl && \
    apt-get clean

# install rocksdb
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libzstd-dev librocksdb-dev && \
    apt-get clean

# install python development tools, setuptools and pip
WORKDIR /opt
RUN wget https://bitbucket.org/pypy/pypy/downloads/pypy3-v6.0.0-linux64.tar.bz2
RUN tar fxv pypy3-v6.0.0-linux64.tar.bz2
RUN ln -s /opt/pypy3-v6.0.0-linux64/bin/pypy3 /usr/bin/pypy3
RUN pypy3 -m ensurepip
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python3 get-pip.py

# configure locale
RUN apt-get update && apt-get install -y locales
RUN locale-gen en_US.UTF-8 && dpkg-reconfigure --frontend noninteractive locales
ENV LC_ALL="en_US.UTF-8" LANG="en_US.UTF-8"

EXPOSE 22 80 443 38291 38391 38491 8000

### set up code
RUN mkdir /code
WORKDIR /code

# docker build --build-arg CACHEBUST=$(date +%s) .
ARG CACHEBUST=1
RUN git clone https://github.com/QuarkChain/pyquarkchain.git

# py dep
RUN pypy3 -m pip install -r pyquarkchain/requirements.txt
RUN python3 -m pip install -r pyquarkchain/requirements.txt

# build qkchash c++ lib
WORKDIR /code/pyquarkchain/qkchash
RUN make

ENV PYTHONPATH /code/pyquarkchain
ENV QKCHASHLIB /code/pyquarkchain/qkchash/libqkchash.so

WORKDIR /code/pyquarkchain
