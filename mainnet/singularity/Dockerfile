FROM python:3.13-slim
LABEL maintainer="quarkchain"

# PREBUILT=true: download pre-built native extensions from S3 (smaller image, faster build)
# PREBUILT=false: install build tools + Rust and compile extensions locally
ARG PREBUILT=false

RUN apt-get update && apt-get install -y \
    git \
    curl \
    libssl3 \
    libbz2-1.0 \
    libgflags2.2 \
    liblz4-1 \
    librocksdb-dev \
    libsnappy1v5 \
    libzstd1 \
    zlib1g \
    vim \
  && if [ "$PREBUILT" != "true" ]; then \
       apt-get install -y \
         build-essential \
         gcc \
         g++ \
         make \
         python3-dev \
         pkg-config \
         libssl-dev \
         libbz2-dev \
         libgflags-dev \
         liblz4-dev \
         libsnappy-dev \
         libzstd-dev \
         zlib1g-dev; \
     fi \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN git clone https://github.com/QuarkChain/pyquarkchain.git /code/pyquarkchain
WORKDIR /code/pyquarkchain

ARG GIT_TAG
RUN echo "checkout to $GIT_TAG" && git checkout $GIT_TAG

RUN pip install -r requirements.txt

RUN if [ "$PREBUILT" != "true" ]; then \
      curl https://sh.rustup.rs -sSf | sh -s -- -y \
      && export PATH="/root/.cargo/bin:$PATH" \
      && rustup toolchain install stable \
      && rustup default stable \
      && python setup.py build_ext --inplace; \
    fi

# prebuilt mode: download pre-built ethash extensions from S3
RUN if [ "$PREBUILT" = "true" ]; then \
      curl -fsSL https://s3-us-west-2.amazonaws.com/pyqkcmainnet/ethash_cy.cpython-313-x86_64-linux-gnu.so \
           -o /code/pyquarkchain/ethereum/pow/ethash_cy.cpython-313-x86_64-linux-gnu.so \
      && curl -fsSL https://s3-us-west-2.amazonaws.com/pyqkcmainnet/ethash_rs.cpython-313-x86_64-linux-gnu.so \
           -o /code/pyquarkchain/ethereum/pow/ethash_rs.cpython-313-x86_64-linux-gnu.so; \
    fi

ADD https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so \
    /code/pyquarkchain/qkchash/

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
