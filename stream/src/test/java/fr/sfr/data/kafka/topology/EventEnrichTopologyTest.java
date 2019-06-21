package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import fr.sfr.data.kafka.model.Iptv;
import fr.sfr.data.kafka.model.Client;
import fr.sfr.data.kafka.model.ClientIptv;
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

public class EventEnrichTopologyTest {

    private Topic<String, Iptv> SOURCEEVENTIPTV;
    private Topic<String, Client> SOURCEREFCLIENT;

    private Topic<String,ClientIptv > SINK;

    private Map<String, Object> configs = new HashMap<String, Object>() {{
       put("schema.registry.url", "http://dummy");
       put("specific.avro.reader", true);
    }};

    @Before
    public void setUp() {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        SOURCEEVENTIPTV = new Topic<>("source_iptv", Serdes.String(), new SpecificAvroSerde<>(client));
        SOURCEREFCLIENT = new Topic<>("source_client", Serdes.String(), new SpecificAvroSerde<>(client));

        SINK = new Topic<>("sink", Serdes.String(), new SpecificAvroSerde<>(client));
    }

    @Test
    public void test() {

        TopicRegistry registry = new TopicRegistry();
        registry.register(Topics.EventIptvAvro, SOURCEEVENTIPTV);
        registry.register(Topics.ReferentialClientAvro, SOURCEREFCLIENT);
        registry.register(Topics.AggregateClientIptv, SINK);
        registry.configure(configs);

        EventEnrichTopology builder = new EventEnrichTopology();
        Topology topology = builder.build(configs, registry);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-event-enrich");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        TopologyTestDriver driver = new TopologyTestDriver(topology, props);

        ConsumerRecordFactory<String, Iptv> factoryIptv = new ConsumerRecordFactory<>(
                SOURCEEVENTIPTV.name(),
                SOURCEEVENTIPTV.keySerde().serializer(),
                SOURCEEVENTIPTV.valueSerde().serializer());

        ConsumerRecordFactory<String, Client> factoryClient = new ConsumerRecordFactory<>(
                SOURCEREFCLIENT.name(),
                SOURCEREFCLIENT.keySerde().serializer(),
                SOURCEREFCLIENT.valueSerde().serializer());

        Client refclient = Client.newBuilder().setMac("mac9659").setClientId("abc").build();

        Iptv iptvevent = Iptv.newBuilder().setMac("mac9659").build();

        driver.pipeInput(factoryClient.create(SOURCEREFCLIENT.name(), "mac9659", refclient));
        driver.pipeInput(factoryIptv.create(SOURCEEVENTIPTV.name(), "foo", iptvevent));

        ProducerRecord<String, ClientIptv> record = driver.readOutput(
                SINK.name(),
                SINK.keySerde().deserializer(),
                SINK.valueSerde().deserializer());

        Assert.assertEquals("mac9659", record.value().getIptvmac());
        Assert.assertEquals("abc", record.value().getClientid());
        Assert.assertEquals("mac9659", record.value().getClientmac());





        System.out.println(record);

    }

}