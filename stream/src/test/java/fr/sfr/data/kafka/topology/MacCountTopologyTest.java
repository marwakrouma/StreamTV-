package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import fr.sfr.data.kafka.model.Iptv;
import fr.sfr.data.kafka.model.WindowCount;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class MacCountTopologyTest {

    private Topic<String, Iptv> SOURCE;

    private Topic<String, WindowCount> SINK;

    private Map<String, Object> configs = new HashMap<String, Object>() {{
        put("schema.registry.url", "http://dummy");
        put("specific.avro.reader", true);
    }};

    @Before
    public void setUp() {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        SOURCE = new Topic<>("source", Serdes.String(), new SpecificAvroSerde<>(client));
        SINK = new Topic<>("sink", Serdes.String(), new SpecificAvroSerde<>(client));
    }

    @Test
    public void test() {

        TopicRegistry registry = new TopicRegistry();
        registry.register(Topics.EventIptvAvro, SOURCE);
        registry.register(Topics.Audience, SINK);
        registry.configure(configs);

        MacCountTopology builder = new MacCountTopology();

        Topology topology = builder.build(configs, registry);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        TopologyTestDriver driver = new TopologyTestDriver(topology, props);


        ConsumerRecordFactory<String, Iptv> factory = new ConsumerRecordFactory<>(
                SOURCE.name(),
                SOURCE.keySerde().serializer(),
                SOURCE.valueSerde().serializer());

        Iptv event1 = Iptv.newBuilder().setGroupId("channel1").setMac("mac_001").build();
        Iptv event2 = Iptv.newBuilder().setGroupId("channel1").setMac("mac_002").build();
        Iptv event3 = Iptv.newBuilder().setGroupId("channel2").setMac("mac_001").build();
        Iptv event4 = Iptv.newBuilder().setGroupId("channel2").setMac("mac_002").build();
        Iptv event5 = Iptv.newBuilder().setGroupId("channel2").setMac("mac_003").build();
        Iptv event6 = Iptv.newBuilder().setGroupId("channel1").setMac("mac_003").build();

        driver.pipeInput(factory.create(event1, 30L * 1000));
        driver.pipeInput(factory.create(event1, 35L * 1000));
        driver.pipeInput(factory.create(event1, 35L * 1000));

        driver.pipeInput(factory.create(event1, 55L * 1000));
        driver.pipeInput(factory.create(event1, 64L * 1000));
        driver.pipeInput(factory.create(event1, 40L * 1000)); // This will be included in the 0-60 timewindow
        driver.pipeInput(factory.create(event6, 65L * 1000));
        driver.pipeInput(factory.create(event6, 40L * 1000)); // This will not be included in the 0-60 timewindow

        driver.pipeInput(factory.create(event2, 80L * 1000)); // This event will trigger suppress buffer flush
        driver.pipeInput(factory.create(event3, 80L * 1000));
        driver.pipeInput(factory.create(event4, 80L * 1000));
        driver.pipeInput(factory.create(event5, 124L * 1000));
        driver.pipeInput(factory.create(event6, 85L * 1000));
        driver.pipeInput(factory.create(event2, 140L * 1000));
        driver.pipeInput(factory.create(event1, 200L * 1000));

        WindowStore<String, Long> store = (WindowStore<String, Long>)driver.getStateStore(MacCountTopology.Client_STORE_COUNT);

        WindowStoreIterator<Long> iterator = store.fetch("channel1", 60000, 120000);
        System.out.println(iterator.next());


        for (int i = 0 ; i < 10 ; i++) {
            ProducerRecord<String, WindowCount> record = driver.readOutput(
                    SINK.name(),
                    SINK.keySerde().deserializer(),
                    SINK.valueSerde().deserializer());

            System.out.println(record);
        }
        driver.close();

    }

}