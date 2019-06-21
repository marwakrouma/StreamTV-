package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.TopologyBuilder;
import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import fr.sfr.data.kafka.model.Iptv;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Topology qui fait le count sur une fenetre de temps de 1 minute.
 * L'utilisation de l'operateur suppress permet d'émettre uniquement le dernier evenement pour chaque clef de la fenetre de temps
 */
public class IptvEventToAvroTopology implements TopologyBuilder {


    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Topology build(final Map<String, Object> configs, final TopicRegistry topicRegistry) {

        final Topic<String, String> sourceTopic = topicRegistry.get(Topics.EventIptvJson);
        final Topic<String, Iptv> sinkTopic =  topicRegistry.get(Topics.EventIptvAvro);

        StreamsBuilder builder = new StreamsBuilder();

        // read the source stream

        final KStream<String, String> jsonToAvroStream = builder.stream(
                sourceTopic.name(),
                Consumed.with(sourceTopic.keySerde(), sourceTopic.valueSerde()));


        jsonToAvroStream.map((k,v) -> {
            try {
                final JsonNode jsonNode = objectMapper.readTree(v);
                Iptv iptv = new Iptv(
                        jsonNode.get("groupId").asText(),
                        jsonNode.get("mac").asText()
                );
                return KeyValue.pair(jsonNode.get("mac").asText(), iptv);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }

        }).filter((k,v) -> v != null).to(
                sinkTopic.name(),
                Produced.with(sinkTopic.keySerde(), sinkTopic.valueSerde()) );

        return builder.build();
    }
}
