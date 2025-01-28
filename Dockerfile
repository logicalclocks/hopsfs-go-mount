# Copyright (c) Hopsworks AB. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.

FROM ubuntu:22.04

ARG userid=1000
ARG groupid=1000
ARG user=hopsfs

RUN apt-get -y update && \
    apt-get install -y wget git make build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN  cd /tmp; \
wget https://go.dev/dl/go1.19.1.linux-amd64.tar.gz 


RUN cd /tmp; \
ls -al; \
rm -rf /usr/local/go; \
tar -C /usr/local -xzf go1.19.1.linux-amd64.tar.gz 

RUN groupadd hopsfs --gid ${groupid}; \
useradd -ms /bin/bash hopsfs --uid ${userid} --gid ${groupid};


RUN mkdir /src; \
chown ${user}:${user} /src

ENV PATH=$PATH:/usr/local/go/bin
ENV GOPATH=/go
RUN mkdir /go; \
    chmod 777 /go

USER hopsfs
RUN echo $PATH && \
    echo $GOPATH && \
    go install github.com/golang/mock/mockgen@v1.6.0 && \
    echo "export PATH=$PATH:/go/bin" >> /home/hopsfs/.bashrc
