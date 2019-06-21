package fr.sfr.data.kafka;

import fr.sfr.data.kafka.domain.TopicRegistry;
import org.apache.kafka.streams.Topology;

import java.util.Map;

public interface TopologyBuilder {

    Topology build(final Map<String, Object> configs, final TopicRegistry registry);
}
