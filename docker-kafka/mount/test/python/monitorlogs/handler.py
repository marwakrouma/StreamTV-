#!/usr/bin/env python

import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from producer import Producer
from sender import Sender



class Handler(FileSystemEventHandler):
    def __init__(self,kafka_topic1,kafka_topic2,producer_kafka):
        self.kafka_topic1 =kafka_topic1
        self.kafka_topic2 =kafka_topic2
        self.producer_kafka = producer_kafka

    def on_any_event(self, event):
        if event.is_directory:
            return None
        elif event.event_type == 'created':
            self.run(event)

    def run(self, event):
            # Take any action here when a file is first created.
            print ("Received created event - %s." % event.src_path)
            producer_custom = Producer(event.src_path)
            json_data = producer_custom.get_json()
            json_key = producer_custom.get_key_json(json_data)
            sender_custom = Sender(json_key,json_data,self.producer_kafka)
            sender_custom.send_to_kafka_big(self.kafka_topic1)
            sender_custom.send_to_kafka_small(self.kafka_topic2)
