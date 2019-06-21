package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import fr.sfr.data.kafka.model.Client;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RefClientToAvroTopologyTest {

    private Topic<String, String> SOURCE;

    private Topic<String, Client> SINK;

    private Map<String, Object> configs = new HashMap<String, Object>() {{
       put("schema.registry.url", "http://dummy");
       put("specific.avro.reader", true);
    }};

    @Before
    public void setUp() {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        SOURCE = new Topic<>("source", Serdes.String(), Serdes.String());
        SINK = new Topic<>("sink", Serdes.String(), new SpecificAvroSerde<>(client));
    }

    @Test
    public void test() {

        TopicRegistry registry = new TopicRegistry();
        registry.register(Topics.ReferentialClientJson, SOURCE);
        registry.register(Topics.ReferentialClientAvro, SINK);
        registry.configure(configs);

        RefClientToAvroTopology builder = new RefClientToAvroTopology();
        Topology topology = builder.build(configs, registry);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-ref-client-to-avro");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        TopologyTestDriver driver = new TopologyTestDriver(topology, props);

        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(
                SOURCE.name(),
                SOURCE.keySerde().serializer(),
                SOURCE.valueSerde().serializer());

        driver.pipeInput(factory.create("{\"clientId\":\"ABC\",\"mac\":\"mac1\"}"));

        ProducerRecord<String, Client> record = driver.readOutput(
                SINK.name(),
                SINK.keySerde().deserializer(),
                SINK.valueSerde().deserializer());

        Assert.assertEquals("mac1", record.value().getMac());
        System.out.println(record);

    }

}