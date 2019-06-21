package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.TopologyBuilder;
import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

import fr.sfr.data.kafka.model.Client;
import org.apache.kafka.streams.kstream.KStream;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import java.io.IOException;
import java.util.Map;

/**
 * Topology qui transforme Json en Avro
 */
public class RefClientToAvroTopology implements TopologyBuilder {


    @Override
    public Topology build(final Map<String, Object> configs, final TopicRegistry registry) {

        final Topic<String, Client> clientAvroTopic = registry.get(Topics.ReferentialClientAvro);
        final Topic<String, String> clientJsonTopic = registry.get(Topics.ReferentialClientJson);

        StreamsBuilder builder = new StreamsBuilder();
        final ObjectMapper objectMapper = new ObjectMapper();
        // read the source stream
        final KStream<String, String> jsonToAvroStream = builder.stream(
                clientJsonTopic.name(),
                Consumed.with(clientJsonTopic.keySerde(), clientJsonTopic.valueSerde()));

        jsonToAvroStream.map((k,v) -> {
            try {
                final JsonNode jsonNode = objectMapper.readTree(v);
                Client client = new Client(jsonNode.get("clientId").asText(),
                                            jsonNode.get("mac").asText()
                );
                return KeyValue.pair(k, client);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }

        }).filter((k,v) -> v != null).to(
                clientAvroTopic.name(),
                Produced.with(clientAvroTopic.keySerde(), clientAvroTopic.valueSerde()) );

        return builder.build();
    }
}



