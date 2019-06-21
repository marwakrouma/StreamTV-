package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import fr.sfr.data.kafka.model.Iptv;
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

public class IptvEventToAvroTopologyTest {

    private Topic<String, String> SOURCE;

    private Topic<String, Iptv> SINK;

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
        registry.register(Topics.EventIptvJson, SOURCE);
        registry.register(Topics.EventIptvAvro, SINK);
        registry.configure(configs);

        IptvEventToAvroTopology builder = new IptvEventToAvroTopology();
        Topology topology = builder.build(configs, registry);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-iptv-event-to-avro");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        TopologyTestDriver driver = new TopologyTestDriver(topology, props);

        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(
                SOURCE.name(),
                SOURCE.keySerde().serializer(),
                SOURCE.valueSerde().serializer());

        driver.pipeInput(factory.create("{\"shw\":\"mac9659\", \"sfw\":\"ytg\",\"groupId\":\"mac9659\",\"mac\":\"mac9659\",\"lcn\":\"mac9659\",\"lzd\":\"mac9659\",\"ip\":\"mac9659\",\"rts\":\"ts\",\"ts\":\"mac9659\"}"));

        ProducerRecord<String, Iptv> record = driver.readOutput(
                SINK.name(),
                SINK.keySerde().deserializer(),
                SINK.valueSerde().deserializer());

        Assert.assertEquals("mac9659", record.value().getMac());
        System.out.println(record);

    }

}