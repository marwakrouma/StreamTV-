package fr.sfr.data.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class Launcher implements CommandLineRunner {

    @Autowired
    Topology topology;

    @Autowired
    StreamsConfig streamsConfig;

    public static void main(final String[] args) {
        new SpringApplication(Launcher.class)
                .run(args)
                .registerShutdownHook();
    }

    public void run(String... args) { new KafkaStreams(topology, streamsConfig).start(); }
}
