# Copyright (c) Hopsworks AB. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.

FROM centos:centos7

ARG userid=1000
ARG groupid=1000
ARG user=hopsfs

RUN ulimit -n 1024000 && \
   yum -y update && \
   yum -y install wget git make

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

RUN echo "export PATH=$PATH:/usr/local/go/bin" >> /home/hopsfs/.bashrc

USER hopsfs
