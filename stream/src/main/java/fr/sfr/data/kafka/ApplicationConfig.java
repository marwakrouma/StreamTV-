package fr.sfr.data.kafka;

import fr.sfr.data.kafka.domain.Topic;
import fr.sfr.data.kafka.domain.TopicRegistry;
import fr.sfr.data.kafka.domain.Topics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableAutoConfiguration
@EnableConfigurationProperties
@Validated
public class ApplicationConfig {

    private static final String KAFKA_PREFIX_PROPERTY = "streams.";

    @Autowired
    private Environment env;

    @Value("${streams.application.id}")
    public String applicationId;

    @Value("${streams.num.threads:1}")
    public int numThreads;

    @Value("${bootstrap.servers}")
    public String bootstrapServer;

    @Value("${topology.class}")
    public String topologyClass;

    @Bean
    @ConfigurationProperties
    public TopicConfig topicConfig() {
        return new TopicConfig();
    }

    @Bean
    @ConfigurationProperties
    public TopologyConfig topologyConfig() {
        return new TopologyConfig();
    }

    private static Map<String, String> getAllKnownProperties(Environment env) {
        Map<String, Object> allProperties = new HashMap<>();
        Map<String, String> kafkaProperties = new HashMap<>();
        if (env instanceof ConfigurableEnvironment) {
            //Get all Props
            for (PropertySource<?> propertySource : ((ConfigurableEnvironment) env)
                    .getPropertySources()) {
                if (propertySource instanceof EnumerablePropertySource) {
                    for (String key : ((EnumerablePropertySource) propertySource).getPropertyNames()) {
                        allProperties.put(key, propertySource.getProperty(key));
                    }
                }
            }
            //Filter on kafka and remove prefix
            allProperties.forEach((k, v) -> {
                if (k.startsWith(KAFKA_PREFIX_PROPERTY)) {
                    kafkaProperties
                            .put((k.substring(KAFKA_PREFIX_PROPERTY.length())), v.toString());
                }
            });
        }
        return kafkaProperties;
    }

    public TopicRegistry topicRegistry() {
        TopicRegistry registry = new TopicRegistry();

        registry.register(Topics.ReferentialClientAvro, new Topic<>(
                topicConfig().getTopic().get(Topics.ReferentialClientAvro),
                Serdes.String(),
                new SpecificAvroSerde<>()));

        registry.register(Topics.AggregateClientIptv, new Topic<>(
                topicConfig().getTopic().get(Topics.AggregateClientIptv),
                Serdes.String(),
                new SpecificAvroSerde<>()));

        registry.register(Topics.EventIptvJson, new Topic<>(
                topicConfig().getTopic().get(Topics.EventIptvJson),
                Serdes.String(),
                Serdes.String()));

        registry.register(Topics.EventIptvAvro, new Topic<>(
                topicConfig().getTopic().get(Topics.EventIptvAvro),
                Serdes.String(),
                new SpecificAvroSerde<>()));

        registry.register(Topics.Audience, new Topic<>(
                topicConfig().getTopic().get(Topics.Audience),
                Serdes.String(),
                Serdes.String()));

        registry.register(Topics.ReferentialClientJson, new Topic<>(
                topicConfig().getTopic().get(Topics.ReferentialClientJson),
                Serdes.String(),
                Serdes.String()));

        registry.configure(streamsConfig().originals());
        return registry;
    }

    @Bean
    public Topology topology() {
        try {
            Class<? extends  TopologyBuilder> clazz = Class.forName(topologyClass, true, ApplicationConfig.class.getClassLoader())
                    .asSubclass(TopologyBuilder.class);
            TopologyBuilder builder = clazz.getDeclaredConstructor().newInstance();

            String configsKey = clazz.getSimpleName().toLowerCase();
            Map<String, Object> configs = topologyConfig().getConfigs().getOrDefault(configsKey, new HashMap<>());

            return builder.build(configs, topicRegistry());
        } catch (NoSuchMethodException exception) {
            throw new RuntimeException("Unknown topology builder class : " + topologyClass);
        } catch (ReflectiveOperationException exception) {
            throw new RuntimeException("Cannot create new instance for class '" + topologyClass + "'", exception);
        }
    }

    @Bean
    public StreamsConfig streamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        props.putAll(getAllKnownProperties(env));

        return new StreamsConfig(props);
    }

    public static class TopicConfig {

        public Map<String, String> topic = new HashMap<>();

        public Map<String, String> getTopic() {
            return topic;
        }

    }

    public static class TopologyConfig {

        public Map<String,  Map<String, Object>> configs = new HashMap<>();

        public Map<String,  Map<String, Object>> getConfigs() {
            return configs;
        }

    }
}
