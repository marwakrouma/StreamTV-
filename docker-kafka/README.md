# Démarrage infra local

```
docker-compose -f docker-compose.yml up -d

docker-compose -f docker-compose.yml ps

Name                  Command            State              Ports
----------------------------------------------------------------------------------
base              /bin/sh                     Up      8083/tcp, 9092/tcp
kafka-1           /etc/confluent/docker/run   Up      9092/tcp
ksql-cli          /bin/sh                     Up
ksql-datagen      /bin/sh                     Up
ksql-server       /etc/confluent/docker/run   Up      0.0.0.0:8088->8088/tcp
schema-registry   /etc/confluent/docker/run   Up      8081/tcp
zk-1              /etc/confluent/docker/run   Up      2181/tcp, 2888/tcp, 3888/tcp
```

Kafka-HQ => http://localhost:8081/


# Connection dans le container base pour execution de commande kafka

```
docker-compose -f docker-compose.yml exec base bash
```

# Connection dans le container ksql-cli pour execution de commande ksql

```
docker-compose -f docker-compose.yml exec ksql-cli ksql http://ksql-server:8088

LIST topics;
```

# Démarrage infra local monitoring

```
docker-compose -f docker-compose-monitoring.yml up -d

docker-compose -f docker-compose-monitoring.yml ps

Name                      Command               State            Ports
---------------------------------------------------------------------------------------
cadvisor               /usr/bin/cadvisor -logtostderr   Up       0.0.0.0:8080->8080/tcp
control-center         /etc/confluent/docker/run        Up       0.0.0.0:9021->9021/tcp
grafana                /run.sh                          Up       0.0.0.0:3000->3000/tcp
kafka-1-jmx-exporter   /opt/entrypoint.sh               Up
monitoring             /opt/entrypoint.sh               Exit 7
prometheus             /bin/prometheus --config.f ...   Up       0.0.0.0:9090->9090/tcp
zk-1-jmx-exporter      /opt/entrypoint.sh               Up
```

Control center => http://localhost:9021/

Grafana => http://localhost:3000/

Prometheus => http://localhost:9090/

Cadvisor => http://localhost:8080/


Relancer la commande suivante si les dashboards ne sont pas présents :

```
docker-compose -f docker-compose-monitoring.yml up monitoring
```

# Ingestion de données générées => HELLO WORLD impressions

```
docker-compose -f docker-compose.yml exec ksql-datagen bash

ksql-datagen bootstrap-server=kafka-1:9092 schema=/test/datagen/impressions.avro format=json topic=impressions key=impressionid
```

```
docker-compose -f docker-compose.yml exec base bash

kafka-console-consumer --bootstrap-server kafka-1:9092 --topic impressions
```

```
docker-compose -f docker-compose.yml exec ksql-cli ksql http://ksql-server:8088

CREATE STREAM impressions (viewtime BIGINT, key VARCHAR, userid VARCHAR, adid VARCHAR) WITH (KAFKA_TOPIC='impressions', VALUE_FORMAT='json');
CREATE STREAM impressions2 WITH (KAFKA_TOPIC='impressions2', PARTITIONS=1) as select * from impressions;
```

```
docker-compose -f docker-compose.yml exec base bash

kafka-console-consumer --bootstrap-server kafka-1:9092 --topic IMPRESSIONS2 --property print.key=true --from-beginning
```

# KSQL server en mode headless

Le mode headless s'active automatiquement à partir du moment où KSQL_KSQL_QUERIES_FILE est défini dans le fichier docker-compose.yml.
Le ksql-cli ne peut plus être utilisé.


kafka-console-producer --broker-list kafka-1:9092 --topic topic_in --property "parse.key=true" --property "key.separator=:"
kafka-console-consumer --bootstrap-server kafka-1:9092 --topic topic_out --property print.key=true