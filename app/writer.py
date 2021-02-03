# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""Reader class based on MQTT

This implementation does its best to follow the Robert Martin's Clean code guidelines.
The comments follows the Google Python Style Guide:
    https://github.com/google/styleguide/blob/gh-pages/pyguide.md
"""

__copyright__ = 'Copyright 2021, FCRlab at University of Messina'
__author__ = 'Lorenzo Carnevale <lcarnevale@unime.it>'
__credits__ = ''
__description__ = 'Reader class based on MQTT'


import json
import socket
import logging
import threading
import persistqueue
import paho.mqtt.client as mqtt


class Writer:

    def __init__(self, host, port, topics, mutex, verbosity):
        self.__host = host
        self.__port = port
        self.__topics = topics
        self.__mutex = mutex
        self.__client_name = '%s-sub' % (socket.gethostname())
        self.__writer = None
        self.__setup_logging(verbosity)

    def __setup_logging(self, verbosity):
        format = "%(asctime)s %(filename)s:%(lineno)d %(levelname)s - %(message)s"
        filename='log/mqtt2influx.log'
        datefmt = "%d/%m/%Y %H:%M:%S"
        level = logging.INFO
        if (verbosity):
            level = logging.DEBUG
        logging.basicConfig(filename=filename, filemode='a', format=format, level=level, datefmt=datefmt)


    def setup(self):
        self.__writer = threading.Thread(
            target = self.__writer_job, 
            args = (self.__host, self.__port, self.__topics)
        )

    def __writer_job(self, host, port, topics):
        # SQLite objects created in a thread can only be used in that same thread.
        self.__mutex.acquire()
        self.__queue = persistqueue.SQLiteQueue('data', multithreading=True, auto_commit=True)
        self.__mutex.release()

        # defining the client and callbacks
        client = mqtt.Client(self.__client_name)
        client.on_connect = self.__on_connect
        client.on_disconnect = self.__on_disconnect
        client.on_message = self.__on_message

        # client.username_pw_set(username=options.username,password=options.password)
        client.connect(host, port)

        try:
            client.loop_forever()
        except KeyboardInterrupt:
            pass
        finally:
            client.disconnect()

    """Connection's callback

    The callback used when the client receives a CONNACK response from the server.
    Subscribing to on_connect() means that the connection is renewed when it is lost.

    Args:
        client (obj:'paho.mqtt.client.Client'): the client instance for this callback
        rc (int): is used for checking that the connection was established
    """
    def __on_connect(self, client, _, __, rc):
        return_code = {
            0: "Connection successful",
            1: "Connection refused – incorrect protocol version",
            2: "Connection refused – invalid client identifier",
            3: "Connection refused – server unavailable",
            4: "Connection refused – bad username or password",
            5: "Connection refused – not authorised",
        }
        if (rc == 0):
            try:
                for topic in self.__topics:
                    logging.debug("Subscription to %s ..." % (topic))
                    result, mid = client.subscribe(topic)
                    if (result != mqtt.MQTT_ERR_SUCCESS):
                        raise RuntimeError(result)
                    logging.info("Subscription to %s ... completed" % (topic))
            except Exception as e:
                logging.error(e)
        else:
            logging.error(return_code[rc])

    """MQTT Disconnection callback

    Args:
        client (obj:'paho.mqtt.client.Client'): the client instance for this callback
        rc (int): is used for checking that the disconnection was done
    """
    def __on_disconnect(self, client, _, rc):
        logging.info('Disconnection successful %s' % (rc))

    """MQTT message received callback

    The callback used when a published message is received from the server.

    Args:
        client (obj:'paho.mqtt.client.Client'): the client instance for this callback
        msg (): an instance of MQTTMessage
    """
    def __on_message(self, client, _, msg):
        try:
            payload = json.loads(msg.payload)
            logging.debug('Received JSON data from %s' % (msg.topic))
            self.__queue.put(payload)
            logging.debug('JSON data insert into the queue')
        except ValueError:
            logging.error('Message received from %s is malformed. JSON required.' % (msg.topic))
        except Exception as e:
            logging.error(e)


    def start(self):
        self.__writer.start()