package fr.sfr.data.kafka.topology;

import fr.sfr.data.kafka.TopologyBuilder;
import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import fr.sfr.data.kafka.model.Iptv;
import fr.sfr.data.kafka.model.WindowCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

/**
 * Topology qui fait le count sur une fenetre de temps de 1 minute.
 * L'utilisation de l'operateur suppress permet d'émettre uniquement le dernier evenement pour chaque clef de la fenetre de temps
 */
public class MacCountTopology implements TopologyBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(MacCountTopology.class);

    static final String Client_STORE_COUNT = "client_store_count";
    static final String DEDUP_STORE_NAME   = "deduplicationAudience";

    @Override
    public Topology build(final Map<String, Object> configs, final TopicRegistry registry) {
        LOG.info("Configs : {}", configs);

        final Topic<String, Iptv> iptvTopic = registry.get(Topics.EventIptvAvro);
        final Topic<String, WindowCount> audienceTopic = registry.get(Topics.Audience);
        final Duration windowSize = Duration.ofMillis((Long.parseLong(configs.getOrDefault("windowSizeMs", "60000").toString())));
        final Duration windowGrace = Duration.ofMillis(Long.parseLong(configs.getOrDefault("windowGracePeriodMs", "5000").toString()));
        final long inactivityPeriod = Long.parseLong(configs.getOrDefault("inactivityMs", "600000").toString());

        //final Duration windowSize = ofMinutes(1);
        //final Duration windowGrace = ofSeconds(5);
        final Duration retentionPeriod = windowSize.plus(windowGrace);

        StreamsBuilder builder = new StreamsBuilder();
        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(DEDUP_STORE_NAME, retentionPeriod , windowSize, false),
                Serdes.String(),
                Serdes.Long()
        );
        builder.addStateStore(dedupStoreBuilder);

        final KStream<String, Iptv> iptvStream = builder.stream(
                iptvTopic.name(),
                Consumed.with(iptvTopic.keySerde(), iptvTopic.valueSerde()));

        final KGroupedStream<String, Iptv> channels = iptvStream
                //.filter( (k, v) ->  v.getLkp() == null || v.getLkp() < inactivityPeriod)
                .selectKey((mac, iptv) -> iptv.getGroupId())
                .transform(() -> new DeduplicationTransformer<>(DEDUP_STORE_NAME, windowSize, (k, v) -> v.getGroupId() + v.getMac()), DEDUP_STORE_NAME)
                .groupByKey(Grouped.with(Serdes.String(), iptvTopic.valueSerde()));

        KTable<Windowed<String>, Long> audienceTable = channels
                .windowedBy(TimeWindows.of(windowSize).grace(windowGrace))
                .count(Materialized.as(Client_STORE_COUNT))
                .suppress(Suppressed.untilWindowCloses(unbounded()));

        KStream<String, String> audienceStream = audienceTable
                .toStream()
                .map( (k, v) -> {

                    String result = "Audience for channel: "  + k.key() + "from " + String.valueOf(k.window().start()) + " To " + String.valueOf(k.window().end()) + "is : " + v;

//                    WindowCount count = WindowCount.newBuilder()
//                            .setChannel(k.key())
//                            .setWindowStart(String.valueOf(k.window().start()))
//                            .setWindowEnd(String.valueOf(k.window().end()))
//                            .setAudience(v)
//                            .build();
                    return KeyValue.pair(k.key(), result);
                });

        audienceStream.to(
                audienceTopic.name(),
                Produced.with(Serdes.String(), Serdes.String())
        );

        return builder.build();
    }


}
