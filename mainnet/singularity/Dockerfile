FROM python:3.7-buster
LABEL maintainer="quarkchain"
RUN sed -i 's/deb.debian.org/archive.debian.org/' /etc/apt/sources.list
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
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

# add qkchash c++ lib
ADD https://s3-us-west-2.amazonaws.com/pyqkcmainnet/libqkchash.so /code/pyquarkchain/qkchash/

EXPOSE 22 38291 38391 38491
ENV PYTHONPATH=/code/pyquarkchain
ENV QKCHASHLIB=/code/pyquarkchain/qkchash/libqkchash.so
