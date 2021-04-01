# MQTT to Influx Microservice
<img src="https://img.shields.io/badge/python-v3-blue" alt="python-v3-blue"> <img src="https://img.shields.io/badge/influxdb-v2.0.3-blue" alt="influxdb-v2.0.3">

A Python implementation of a MQTT to Influx proxy microservice,  intended as support to Internet of Things (IoT) applications. It uses the reader-writer paradigm for consuming messages.

## How to use it
Clone the repository and build it locally using the Dockerfile.
```bash
docker build -t lcarnevale/mqtt2influx .
```

Run the image, defining your configuration directory (i.e. ```/opt/lcarnevale/mqtt2influx```).
```bash
docker run -d --name mqtt2influx \
    -v /opt/lcarnevale/mqtt2influx:/etc/mqtt2influx \
    -v /var/log/lcarnevale:/opt/app/log \
    --net=host \
    lcarnevale/mqtt2influx
```

Use also the option ```--restart unless-stopped ``` if you wanna make it able to start on boot.

## How to debug it
Open the log file for watching what is going on.
```bash
tail -f /var/log/lcarnevale/mqtt2influx.log
```

Publish a MQTT message using your favorite client, formatting data as following.
```json
{
    "measurement": "my-measurement",
    "tags": {
        "region": "my-region",
    },
    "fields": {
        "sensor": "data"
    }
}
```
