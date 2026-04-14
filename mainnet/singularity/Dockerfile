FROM python:3.13-slim
LABEL maintainer="quarkchain"

# install rocksdb and build tools (including Rust build deps)
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    build-essential \
    curl \
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

# Install Rust toolchain (needed to build the ethash_rs extension)
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

# set up code
RUN mkdir /code
WORKDIR /code
RUN git clone https://github.com/QuarkChain/pyquarkchain.git

# switch to target tag
WORKDIR /code/pyquarkchain
ARG GIT_TAG
RUN echo "checkout to $GIT_TAG"
RUN git checkout $GIT_TAG

# Python dependencies
RUN pip install -r requirements.txt

# Build Cython and Rust extensions in-place (ethash acceleration)
RUN python setup.py build_ext --inplace

# Cleanup build dependencies to reduce image size
RUN apt-get purge -y build-essential curl gcc g++ make \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/.cargo /root/.rustup \
       /code/pyquarkchain/ethereum/pow/ethash_rs/target

# add qkchash c++ lib
ADD https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so /code/pyquarkchain/qkchash/

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
