FROM ubuntu:latest

WORKDIR /root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get upgrade -y

# Install python-build dependencies
RUN apt-get install -y git-all make build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

# Install python-build
RUN git clone https://github.com/pyenv/pyenv.git && \
    cd pyenv/plugins/python-build && \
    ./install.sh

# Install python 2.7
RUN /usr/local/bin/python-build 2.7.18 /usr/local/

# Make Directories for Asiaq
RUN mkdir -p /project/asiaq
RUN mkdir -p /project/asiaq_config

# Install Asiaq
## Asiaq Dependencies
RUN apt-get install -y rake rsync

## Copy over asiaq files
WORKDIR /project/asiaq
COPY ./ /project/asiaq

## Actually install asiaq
RUN rake setup:develop

# Copy over AWS configs that Asiaq needs
COPY ./jenkins/base_boto.cfg /root/.aws/config
COPY ./jenkins/base_boto.cfg /root/.boto

# Set our working directory to be the config directory, which we will mount at runtime
WORKDIR /project/asiaq_config
