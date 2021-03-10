package com.udemy.kafka.streaming;

import com.udemy.kafka.streaming.factory.TopologyFactory;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@SpringBootApplication
@Log4j2
public class KafkaStreamsApplication {
    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;
    @Value("${spring.kafka.streams.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.producer.key-serializer}")
    private String producerKeySer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String producerValueSer;
    @Value("${spring.kafka.consumer.key-deserializer}")
    private String consumerKeyDeser;
    @Value("${spring.kafka.consumer.value-deserializer}")
    private String consumerValueDeser;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String consumerAutoOffSetReset;

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner() {
        log.info("running commandLineRunner");

        return args -> {
            final Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerKeySer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerValueSer);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerKeyDeser);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerValueDeser);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffSetReset);

            final KafkaStreams streams = new KafkaStreams(TopologyFactory.getWordsCountTopology(), props);
            streams.cleanUp(); // dont use in prod, this will delete localStore data related to application_id
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        };
    }
}
