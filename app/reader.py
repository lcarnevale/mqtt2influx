# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""Writer class based on InfluxDB

This implementation does its best to follow the Robert Martin's Clean code guidelines.
The comments follows the Google Python Style Guide:
    https://github.com/google/styleguide/blob/gh-pages/pyguide.md
"""

__copyright__ = 'Copyright 2021, FCRlab at University of Messina'
__author__ = 'Lorenzo Carnevale <lcarnevale@unime.it>'
__credits__ = ''
__description__ = 'Writer class based on InfluxDB'


import time
import logging
import threading
import persistqueue
from datetime import datetime
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WritePrecision

class Reader:

    def __init__(self, host, port, token, organization, bucket, mutex, verbosity):
        self.__url = "http://%s:%s" % (host, port)
        self.__token = token
        self.__organization = organization
        self.__bucket = bucket
        self.__mutex = mutex
        self.__reader = None
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
        self.__reader = threading.Thread(
            target = self.__reader_job, 
            args = (self.__url, self.__token, self.__organization, self.__bucket)
        )
    
    def __reader_job(self, url, token, organization, bucket):
        self.__mutex.acquire()
        q = persistqueue.SQLiteQueue('data', multithreading=True, auto_commit=True)
        self.__mutex.release()

        client = InfluxDBClient(url=url, token=token)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        try:
            while (True):
                raw_data = q.get()
                logging.debug("Just got new data")

                logging.debug("Parsing data points")
                data = [
                    {
                        "measurement": raw_data['measurement'],
                        "tags": raw_data['tags'], 
                        "fields": raw_data['fields'], 
                        "time": datetime.utcnow()
                    }
                ]

                write_api.write(bucket, organization, data)
                logging.info("Data into InfluxDB")

                time.sleep(0.3)
        except KeyboardInterrupt:
            pass
        

    def start(self):
        self.__reader.start()