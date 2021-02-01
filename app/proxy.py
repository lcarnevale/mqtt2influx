# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""MQTT to InfluxDB Proxy

This implementation does its best to follow the Robert Martin's Clean code guidelines.
The comments follows the Google Python Style Guide:
    https://github.com/google/styleguide/blob/gh-pages/pyguide.md
"""

__copyright__ = 'Copyright 2021, FCRlab at University of Messina'
__author__ = 'Lorenzo Carnevale <lcarnevale@unime.it>'
__credits__ = ''
__description__ = 'MQTT to InfluxDB Proxy'

import json
import yaml
import socket
import logging
import argparse
import threading
import persistqueue
import paho.mqtt.client as mqtt
from reader import Reader


def writer_job(host, port, topics):
    q = persistqueue.SQLiteQueue('data', auto_commit=True)

    # initializing metadata
    client_name = '%s-sub' % (socket.gethostname())

    # implementing callbacks
    def on_connect(client, _, __, rc):
        """Connection's callback

        The callback for when the client receives a CONNACK response from the server.
        It subscribes to all the topics specified in the topics list.
        Moreover, subscribing in on_connect() means that if we lose the connection
        and reconnect then subscriptions will be renewed.

        Connection Return Codes:
            0: Connection successful
            1: Connection refused – incorrect protocol version
            2: Connection refused – invalid client identifier
            3: Connection refused – server unavailable
            4: Connection refused – bad username or password
            5: Connection refused – not authorised
            6-255: Currently unused.

        Args:
            client(obj:'paho.mqtt.client.Client'): the client instance for this callback;
            rc(int): is used for checking that the connection was established.
        """
        return_code = {
            0: "Connection successful",
            1: "Connection refused – incorrect protocol version",
            2: "Connection refused – invalid client identifier",
            3: "Connection refused – server unavailable",
            4: "Connection refused – bad username or password",
            5: "Connection refused – not authorised",
        }
        print(return_code.get(rc, "Currently unused."))
        try:
            for topic in topics:
                client.subscribe(topic)
                print("Subscribed to %s" % (topic))
        except Exception as e:
            print(e)

    def on_disconnect(client, _, rc):
        """
        Args:
            client(obj:'paho.mqtt.client.Client'): the client instance for this callback;
            rc(int): is used for checking that the disconnection was done.
        """
        print('Disconnection successful %s' % (rc))
        del q

    def on_message(client, _, msg):
        """Message's callback

        The callback for when a PUBLISH message is received from the server.

        Args:
            client(obj:'paho.mqtt.client.Client'): the client instance for this callback;
            msg(): an instance of MQTTMessage.
        """
        try:
            payload = json.loads(msg.payload)
            q.put(payload)
            print('Just written %s - %s' % (msg.topic, payload))
        except ValueError:
            print('Message is malformed. JSON required.')
        except Exception as e:
            print(e)

    # defining the client and callbacks
    client = mqtt.Client(client_name)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # authenticating and connecting to the broker
    # client.username_pw_set(username=options.username,password=options.password)
    client.connect(host, port)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()


def main():
    description = ('%s\n%s' % (__author__, __description__))
    epilog = ('%s\n%s' % (__credits__, __copyright__))
    parser = argparse.ArgumentParser(
        description = description,
        epilog = epilog
    )

    parser.add_argument('-c', '--config',
                        dest='config',
                        help='YAML configuration file',
                        type=str,
                        required=True)

    options = parser.parse_args()

    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

    with open(options.config) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    writer = setup_writer(config['mqtt'])
    reader = setup_reader(config['influx'])
    writer.start()
    reader.start()

def setup_writer(config):
    return threading.Thread(
        target = writer_job, 
        args = (config['host'],config['port'], config['topics'])
    )

def setup_reader(config):
    reader = Reader(config['host'], config['port'], config['token'],
            config['organization'], config['bucket'])
    reader.setup()
    return reader


if __name__ == '__main__':
    main()