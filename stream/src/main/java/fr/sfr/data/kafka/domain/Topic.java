package fr.sfr.data.kafka.domain;

import org.apache.kafka.common.serialization.Serde;

import java.util.Map;
import java.util.Objects;

public class Topic<K, V> {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public Topic(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
        Objects.requireNonNull(name, "name cannot be null");
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public String name (){
        return name;
    }

    public Serde<K> keySerde(){
        return keySerde;
    }
    public Serde<V> valueSerde(){
        return valueSerde;
    }

    public void configure(final Map<String, Object> configs){
        keySerde.configure(configs, true);
        valueSerde.configure(configs, false);
    }

}
