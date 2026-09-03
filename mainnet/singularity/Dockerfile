# syntax=docker/dockerfile:1

FROM python:3.13-slim 
LABEL maintainer="quarkchain"

ARG GIT_REPOSITORY=https://github.com/QuarkChain/pyquarkchain.git
ARG GIT_TAG=master

RUN apt-get update && apt-get install -y \
    build-essential \
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

RUN --mount=type=secret,id=github_token,required=false \
    if ! printf '%s\n' "$GIT_REPOSITORY" \
        | grep -Eiq '^https://github\.com/QuarkChain/[^/]+(\.git)?$'; then \
        echo "GIT_REPOSITORY must be under the QuarkChain organization on GitHub" >&2; \
        exit 1; \
    elif [ -s /run/secrets/github_token ]; then \
        GIT_AUTH="$(printf 'x-access-token:%s' "$(cat /run/secrets/github_token)" | base64 | tr -d '\n')"; \
        git -c http.https://github.com/.extraHeader="Authorization: Basic $GIT_AUTH" \
            clone "$GIT_REPOSITORY" /code/pyquarkchain; \
    else \
        git clone "$GIT_REPOSITORY" /code/pyquarkchain; \
    fi

WORKDIR /code/pyquarkchain

RUN echo "checkout to $GIT_TAG" && git checkout "$GIT_TAG"

RUN pip install -r requirements.txt

RUN curl -fsSL https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so -o /code/pyquarkchain/qkchash/libqkchash.so

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
