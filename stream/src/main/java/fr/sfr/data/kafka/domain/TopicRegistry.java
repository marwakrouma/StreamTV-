package fr.sfr.data.kafka.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TopicRegistry {

    private Map<String, Topic> topics = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <K, V> Topic<K, V> get(final String id) {
        Objects.requireNonNull(id, "id can not be null");
        if (!topics.containsKey(id)) {
            throw new RuntimeException("No topic registered for id '" + id + "': " + topics.keySet());
        }

        return (Topic<K, V>)topics.get(id);
    }

    public <K, V> void register(final String id, final Topic<K, V> topic) {
        topics.put(id, topic);
    }

    public void configure(final Map<String, Object> configs) {
        topics.forEach( (k, v) -> v.configure(configs));
    }
}
