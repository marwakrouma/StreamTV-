#!/usr/bin/env python

class Sender():
    def __init__(self, key, data, producer_kafka):
        self.key = key
        self.data =data
        self.producer_kafka = producer_kafka

    def on_send_success(self, record_metadata):
        print('record metadata:')
        print('- topic:',record_metadata.topic)
        print('- partition:', record_metadata.partition)
        print('- offset:',record_metadata.offset)

    def on_send_error(self, excp):
        log.error('I am an errback', exc_info=excp)

    def send_to_kafka_big(self,kafka_topic):
        self.producer_kafka.send(kafka_topic, key = self.key.encode('utf-8'), value = self.data).add_callback(self.on_send_success).add_errback(self.on_send_error)

    def send_to_kafka_small(self,kafka_topic):
        self.producer_kafka.send(kafka_topic, key = self.key.encode('utf-8'), value = self.data['ts']).add_callback(self.on_send_success).add_errback(self.on_send_error)
