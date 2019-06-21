/*
package fr.sfr.data.kafka;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractTopologyTest<T extends SpecificRecord> {

    private static final String TARGET_TOPOLOGY_DESCRIPTION = "target/topology-description";

    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(Serdes.String().serializer(), Serdes.String().serializer());
    private TopologyTestDriver ttd;

    protected void before(Topology topology) {
        topology = initApplicationConfig(topology);
        ttd = new TopologyTestDriver(topology.getTopology().build(), topology.getProperties());
        toFile(this.getClass().getName() + ".txt", topology.getTopology().build().describe().toString());
    }

    protected SpecificAvroSerde<T> before(Topology topology, Map<String, Schema> schemaMap) {
        topology = initApplicationConfig(topology);
        SpecificAvroSerde<T> specificAvroSerde = initMockeSchemaRegistry(schemaMap);
        topology.setSpecificAvroSerde(specificAvroSerde);
        before(topology);
        return specificAvroSerde;
    }

    protected ConsumerRecord<byte[], byte[]> createRecord(String topic, String key, Instant now) {
        return createRecord(topic, key, "test", now);
    }

    protected ConsumerRecord<byte[], byte[]> createRecord(String topic, String key, String value, Instant now) {
        return recordFactory.create(topic, key, value, now.toEpochMilli());
    }

    protected List<ProducerRecord> readOutput(String topic, Deserializer keyDeserializer, Deserializer valueDeserializer) {
        List<ProducerRecord> results = new ArrayList<>();
        ProducerRecord<?, ?> record;
        while ((record = ttd.readOutput(topic, keyDeserializer, valueDeserializer)) != null) {
            results.add(record);
        }
        return results;
    }

    private Topology initApplicationConfig(Topology topology) {
        Yaml yaml = new Yaml();
        GlobalConfig config = yaml.loadAs(getClass().getResourceAsStream("/application.yml"), GlobalConfig.class);
        ApplicationConfig applicationConfig = config.getConfig();
        topology.setStreamConfigs(applicationConfig);
        return topology;
    }

    protected void pipeInput(List<ConsumerRecord<byte[], byte[]>> records) {
        ttd.pipeInput(records);
    }

    private SpecificAvroSerde<T> initMockeSchemaRegistry(Map<String, Schema> schemas) {
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        for (String subject : schemas.keySet()) {
            try {
                schemaRegistryClient.register(subject, schemas.get(subject));
            } catch (IOException | RestClientException e) {
                e.printStackTrace();
            }
        }
        return new SpecificAvroSerde<>(schemaRegistryClient);
    }

    private void toFile(String fileName, String content) {
        Path topologyFile = Paths.get(TARGET_TOPOLOGY_DESCRIPTION + "/" + fileName);
        try {
            Files.createDirectories(Paths.get(TARGET_TOPOLOGY_DESCRIPTION));
            Files.deleteIfExists(topologyFile);
            Path file = Files.createFile(topologyFile);
            Files.write(file, content.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
*/