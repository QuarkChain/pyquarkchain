FROM python:3.7-buster
LABEL maintainer="quarkchain"
# install rocksdb
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
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

# Cython 3.x was released in mid-2023 and deprecated many older language features which python-rocksdb needed.
# So pip install -r requirements.txt will fail after mid-2023.
# This is an issue caused by an incompatibility between python-rocksdb and newer versions of Cython.
# Downgrade Cython to a compatible version (0.29.x) is a solution for that.
# We can replicate that success by explicitly installing an older Cython version before building.
# Install Cython separately, because adding it to requirements.txt and installing it will cause docker build to fail.
RUN pip install "Cython<3"

# py dep
RUN pip install -r requirements.txt

# add qkchash c++ lib
ADD https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so /code/pyquarkchain/qkchash/

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
