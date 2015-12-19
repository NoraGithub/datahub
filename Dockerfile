# This is a local-build docker image for p2p-dl test

FROM golang:1.5
MAINTAINER Zonesan <chaizs@asiainfo.com>

ENV TIME_ZONE=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TIME_ZONE /etc/localtime && echo $TIME_ZONE > /etc/timezone

ENV SRCPATH $GOPATH/src/github.com/asiainfoLDP/datahub 
ENV PATH $PATH:$GOPATH/bin:$SRCPATH
RUN mkdir $SRCPATH -p
WORKDIR $SRCPATH

ADD . $SRCPATH

RUN mkdir /var/lib/datahub
RUN curl -s https://raw.githubusercontent.com/pote/gpm/v1.3.2/bin/gpm | bash && \
    go build

EXPOSE 35800

CMD $SRCPATH/start.sh


