#!/usr/bin/env python
import json
#docker-compose exec base /bin/bash

#kafka-topics --create --zookeeper zk-1:2181 --topic json_topic_big --partitions 3 --replication-factor 1 --config cleanup.policy=delete
#kafka-topics --create --zookeeper zk-1:2181 --topic json_topic_small --partitions 3 --replication-factor 1 --config cleanup.policy=compact

#kafka-console-consumer --bootstrap-server kafka-1:9092 --topic json_topic_big --property print.key=true
#kafka-console-consumer --bootstrap-server kafka-1:9092 --topic json_topic_small --property print.key=true

class Producer():

    def __init__(self, file):
        self.file = file

    def get_json(self):
        with open(self.file) as json_file:
            data = json.load(json_file)
        return data

    def get_key_json(self, data):
        return data['db'] + " - " + data['entite']
