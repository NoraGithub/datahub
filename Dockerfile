# Datahub Client

FROM golang:1.6.0
MAINTAINER Zonesan <chaizs@asiainfo.com> ; WeimingYuan <yuanwm@asiainfo.com>

ENV TIME_ZONE=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TIME_ZONE /etc/localtime && echo $TIME_ZONE > /etc/timezone

ENV SRCPATH $GOPATH/src/github.com/asiainfoLDP/datahub 
ENV PATH $PATH:$GOPATH/bin:$SRCPATH
RUN mkdir $SRCPATH -p
WORKDIR $SRCPATH

ADD . $SRCPATH

RUN mkdir /var/lib/datahub

#RUN curl -s https://raw.githubusercontent.com/pote/gpm/v1.3.2/bin/gpm | bash && \
#    go build

RUN go build

EXPOSE 35600
EXPOSE 35800

CMD $SRCPATH/start.sh


