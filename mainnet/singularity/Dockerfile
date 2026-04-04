FROM python:3.13-slim
LABEL maintainer="quarkchain"
# install rocksdb
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    make \
    git \
    libbz2-dev \
    libgflags-dev \
    liblz4-dev \
    librocksdb-dev \
    libsnappy-dev \
    libzstd-dev \
    zlib1g-dev \
    vim \
  && rm -rf /var/lib/apt/lists/*
# set up code
RUN mkdir /code
WORKDIR /code
RUN git clone https://github.com/QuarkChain/pyquarkchain.git

# switch to target tag
WORKDIR /code/pyquarkchain
ARG GIT_TAG
RUN echo "checkout to $GIT_TAG"
RUN git checkout $GIT_TAG

# py dep
RUN pip install -r requirements.txt

# cleanup build dependencies to reduce image size
RUN apt-get purge -y build-essential gcc g++ make \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# add qkchash c++ lib
ADD https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so /code/pyquarkchain/qkchash/

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
