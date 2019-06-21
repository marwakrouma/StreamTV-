package fr.sfr.data.kafka.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Set<String> SerDes
 */
@Component
public class HashsetSerdes implements Serde<Set<String>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Nothing to do.
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Set<String>> serializer() {
        return new HashSetSerializer();
    }

    @Override
    public Deserializer<Set<String>> deserializer() {
        return new HashSetDeserializer();
    }

    public static class HashSetSerializer implements Serializer<Set<String>> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            //Nothing to do.
        }

        @Override
        public byte[] serialize(String topic, Set<String> data) {
            String values = String.join("|", data);
            return values.getBytes(UTF_8);
        }

        @Override
        public void close() {
        }
    }

    public static class HashSetDeserializer implements Deserializer<Set<String>> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            //Nothing to do.
        }

        @Override
        public Set<String> deserialize(String topic, byte[] data) {
            String decoded = new String(data, UTF_8);
            String[] values = decoded.split("\\|");
            return new HashSet<>(Arrays.asList(values));
        }

        @Override
        public void close() {
        }
    }
}