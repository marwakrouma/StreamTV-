CREATE STREAM impressions4 (viewtime BIGINT, key VARCHAR, userid VARCHAR, adid VARCHAR) WITH (KAFKA_TOPIC='impressions', VALUE_FORMAT='avro');
CREATE STREAM impressions2 WITH (KAFKA_TOPIC='impressions2', PARTITIONS=1) as select * from impressions;
CREATE TABLE impressions3 as select userid, count(*) from impressions2 WINDOW TUMBLING (SIZE 2 minutes) group by userid;
