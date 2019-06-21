#!/usr/bin/env python

import time
from watchdog.events import FileSystemEventHandler
from handler import Handler

class Watcher():

    def __init__(self, observer, directory_to_watch, kafka_topic1, kafka_topic2, producer_kafka):
        self.observer = observer
        self.directory_to_watch = directory_to_watch
        self.kafka_topic1 =kafka_topic1
        self.kafka_topic2 =kafka_topic2
        self.producer_kafka = producer_kafka

    def run(self):
        event_handler = Handler(self.kafka_topic1,self.kafka_topic2,self.producer_kafka)
        self.observer.schedule(event_handler, self.directory_to_watch, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print ("Error")

        self.observer.join()
