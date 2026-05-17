FROM python:3.13-slim 
LABEL maintainer="quarkchain"

RUN apt-get update && apt-get install -y \
    gcc \
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

RUN curl -fsSL https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so -o /code/pyquarkchain/qkchash/libqkchash.so


EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
