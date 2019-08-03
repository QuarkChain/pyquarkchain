FROM python:3.7-buster
LABEL maintainer="quarkchain"
# install rocksdb
RUN apt-get update && apt-get install -y \
    libbz2-dev \
    libgflags-dev \
    liblz4-dev \
    librocksdb-dev \
    libsnappy-dev \
    libzstd-dev \
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*
# set up code
RUN mkdir /code
WORKDIR /code
RUN git clone --branch mainnet1.1.0 https://github.com/QuarkChain/pyquarkchain.git

# py dep
RUN pip install -r pyquarkchain/requirements.txt

# add qkchash c++ lib
ADD https://s3-us-west-2.amazonaws.com/qkcmainnet/libqkchash.so /code/pyquarkchain/qkchash/

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH /code/pyquarkchain
ENV QKCHASHLIB /code/pyquarkchain/qkchash/libqkchash.so
WORKDIR /code/pyquarkchain
