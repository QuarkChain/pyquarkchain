FROM python:3.7-buster

LABEL maintainer="quarkchain"

# install rocksdb
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get -y install libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libzstd-dev librocksdb-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# set up code
RUN mkdir /code
WORKDIR /code

# docker build --build-arg CACHEBUST=$(date +%s) .
ARG CACHEBUST=1
RUN git clone https://github.com/QuarkChain/pyquarkchain.git

# py dep
RUN pip install -r pyquarkchain/requirements.txt

# build qkchash c++ lib
WORKDIR /code/pyquarkchain/qkchash
RUN make

EXPOSE 22 38291 38391 38491

ENV PYTHONPATH /code/pyquarkchain
ENV QKCHASHLIB /code/pyquarkchain/qkchash/libqkchash.so

WORKDIR /code/pyquarkchain
