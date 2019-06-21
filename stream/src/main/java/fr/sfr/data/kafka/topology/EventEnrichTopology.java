package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.TopologyBuilder;
import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

import static java.time.Duration.ofSeconds;

import fr.sfr.data.kafka.model.Client;
import fr.sfr.data.kafka.model.Iptv;
import fr.sfr.data.kafka.model.ClientIptv;

import java.util.Map;

/**
 * Topology qui fait le count sur une fenetre de temps de 1 minute.
 * L'utilisation de l'operateur suppress permet d'émettre uniquement le dernier evenement pour chaque clef de la fenetre de temps
 */
public class EventEnrichTopology implements TopologyBuilder {

    static final String Client_STORE = "clientstoreenrich";

    public Topology build(final Map<String, Object> configs, final TopicRegistry registry) {

        final Topic<String, Iptv> iptvTopic = registry.get(Topics.EventIptvAvro);
        final Topic<String, Client> clientTopic = registry.get(Topics.ReferentialClientAvro);
        final Topic<String, ClientIptv> aggTopic = registry.get(Topics.AggregateClientIptv);

        StreamsBuilder builder = new StreamsBuilder();
        // Get the stream of iptv events
        final KStream<String, Iptv> iptvStream = builder.stream(
                iptvTopic.name(),
                Consumed.with(iptvTopic.keySerde(), iptvTopic.valueSerde()));

        // Create a global table for customers. The data from this global table
        // will be fully replicated on each instance of this application.
        final GlobalKTable<String, Client>
                clients =
                builder.globalTable(clientTopic.name(), Materialized.<String, Client, KeyValueStore<Bytes, byte[]>>as(Client_STORE)
                        .withKeySerde(clientTopic.keySerde())
                        .withValueSerde(clientTopic.valueSerde()));

        // Join the iptv stream to the clients global table. As this is global table
        // we can use a non-key based join with out needing to repartition the input stream
        final KStream<String, ClientIptv> clientiptvStream = iptvStream.join(clients,
                (mac, iptvevent ) -> iptvevent.getMac(),
                (iptv, client) -> new ClientIptv(client.getMac(),iptv.getMac(), client.getClientId()));
        // write the enriched order to the enriched-order topic
        clientiptvStream.to(
                aggTopic.name(),
                Produced.with(aggTopic.keySerde(), aggTopic.valueSerde()));

        return builder.build();
    }
}
