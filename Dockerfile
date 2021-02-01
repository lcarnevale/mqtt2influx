FROM alpine:3.13
LABEL maintainer="Lorenzo Carnevale"
LABEL email="lcarnevale@unime.it"

COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app

RUN rm -rf /var/cache/apk/* \
		rm -rf /tmp/*
RUN apk update && \
		apk add python3 python3-dev py3-pip && \
		pip3 install --upgrade pip && \
		pip3 install -r requirements.txt && \
		rm -rf /var/cache/apk/* \
		rm -rf /tmp/*
RUN mkdir -p /var/log/mqtt2influx

COPY app /opt/app

CMD ["sh", "-c", "python3 proxy.py -v -c /etc/mqtt2influx/config.yaml"]