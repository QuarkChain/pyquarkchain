ARG PREBUILT=false

# ---- base: common runtime deps + code ----
FROM python:3.13-slim AS base
LABEL maintainer="quarkchain"

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
  && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN git clone https://github.com/QuarkChain/pyquarkchain.git /code/pyquarkchain
WORKDIR /code/pyquarkchain

ARG GIT_TAG
RUN echo "checkout to $GIT_TAG" && git checkout $GIT_TAG

RUN pip install -r requirements.txt

# ---- prebuilt-false: compile extensions from source (supports amd64 & arm64) ----
FROM base AS prebuilt-false

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
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
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y \
  && export PATH="/root/.cargo/bin:$PATH" \
  && rustup toolchain install stable \
  && rustup default stable \
  && python setup.py build_ext --inplace

# Build libqkchash.so from source so this works on any architecture.
RUN make -C qkchash libqkchash.so

# ---- prebuilt-true: download pre-built extensions from S3 (amd64 only) ----
FROM base AS prebuilt-true

# TARGETARCH is auto-populated by BuildKit. Refuse ARM/other archs because the
# S3 artifacts are compiled for x86_64 and dlopen will fail at runtime.
ARG TARGETARCH
RUN if [ -n "$TARGETARCH" ] && [ "$TARGETARCH" != "amd64" ]; then \
      echo "ERROR: PREBUILT=true only supports linux/amd64 (S3 artifacts are x86_64-only)." >&2; \
      echo "       Build with PREBUILT=false for $TARGETARCH." >&2; \
      exit 1; \
    fi

RUN curl -fsSL https://s3-us-west-2.amazonaws.com/pyqkcmainnet/ethash_cy.cpython-313-x86_64-linux-gnu.so \
         -o /code/pyquarkchain/ethereum/pow/ethash_cy.cpython-313-x86_64-linux-gnu.so \
  && curl -fsSL https://s3-us-west-2.amazonaws.com/pyqkcmainnet/ethash_rs.cpython-313-x86_64-linux-gnu.so \
         -o /code/pyquarkchain/ethereum/pow/ethash_rs.cpython-313-x86_64-linux-gnu.so \
  && curl -fsSL https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so \
         -o /code/pyquarkchain/qkchash/libqkchash.so

# ---- final: selected by PREBUILT arg ----
FROM prebuilt-${PREBUILT}

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
