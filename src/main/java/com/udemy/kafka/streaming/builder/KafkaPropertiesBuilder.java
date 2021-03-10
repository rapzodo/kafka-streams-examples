package com.udemy.kafka.streaming.builder;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.util.Assert;

import java.util.Properties;

public class KafkaPropertiesBuilder {

    private String applicationId;
    private String bootstrapServer;
    private Class defaultKeySerdeClass;
    private Class defaultValueSerdeClass;
    private String consumerAutoOffsetReset;
    private Class producerKeySerializerClass;
    private Class producerValueSerializerClass;
    private String producerIdempotent;
    private Class consumerKeyDeserializerClass;
    private Class consumerValueDeserializerClass;
    private String processingGuarantee;

    public KafkaPropertiesBuilder withApplicationId(String propertyValue) {
        applicationId = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withBootstrapServer(String propertyValue) {
        bootstrapServer = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withDefaultKeySerde(Class propertyValue) {
        defaultKeySerdeClass = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withDefaultValueSerde(Class propertyValue) {
        defaultValueSerdeClass = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withProducerKeySerializer(Class propertyValue) {
        producerKeySerializerClass = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withProducerValueSerializer(Class propertyValue) {
        producerValueSerializerClass = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withConsumerKeyDeserializer(Class propertyValue) {
        consumerKeyDeserializerClass = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withConsumerValueDeserializer(Class propertyValue) {
        consumerValueDeserializerClass = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withConsumerAutoOffsetReset(String propertyValue) {
        consumerAutoOffsetReset = propertyValue;
        return this;
    }public KafkaPropertiesBuilder withProcessingGuarantee(String propertyValue) {
        processingGuarantee = propertyValue;
        return this;
    }

    public KafkaPropertiesBuilder withProducerIdempotentEnabled(String propertyValue) {
        producerIdempotent = propertyValue;
        return this;
    }

    public Properties build() {
        Assert.notNull(applicationId,"application Id cannot be null");
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer == null ? "localhost:9092" : bootstrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, defaultKeySerdeClass == null ? Serdes.String().getClass() : defaultKeySerdeClass);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, defaultValueSerdeClass == null ? Serdes.String().getClass() : defaultKeySerdeClass);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerKeySerializerClass == null ? Serdes.String().getClass() : producerKeySerializerClass);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerValueSerializerClass == null ? Serdes.String().getClass() : producerValueSerializerClass);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerKeyDeserializerClass == null ? Serdes.String().getClass() : consumerKeyDeserializerClass) ;
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerValueDeserializerClass == null ? Serdes.String().getClass() : consumerValueDeserializerClass);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffsetReset  == null ? Topology.AutoOffsetReset.EARLIEST.name().toLowerCase() : consumerAutoOffsetReset);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee == null ? StreamsConfig.EXACTLY_ONCE : processingGuarantee);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producerIdempotent == null ? true : producerIdempotent);
        return props;
    }
}
