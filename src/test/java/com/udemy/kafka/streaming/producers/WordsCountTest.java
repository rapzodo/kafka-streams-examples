package com.udemy.kafka.streaming.producers;

import com.udemy.kafka.streaming.builder.KafkaPropertiesBuilder;
import com.udemy.kafka.streaming.factory.TopologyFactory;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.udemy.kafka.streaming.Topics.WORDS_COUNT_SINK;
import static com.udemy.kafka.streaming.Topics.WORDS_COUNT_SOURCE;

public class WordsCountTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, Long> testOutputTopic;

    @BeforeEach
    private void setup() {
        final Properties props = new KafkaPropertiesBuilder().withApplicationId("word-count-application").build();
        testDriver = new TopologyTestDriver(TopologyFactory.getWordsCountTopology(), props);
        testInputTopic = testDriver.createInputTopic(WORDS_COUNT_SOURCE, new StringSerializer(), new StringSerializer());
        testOutputTopic = testDriver.createOutputTopic(WORDS_COUNT_SINK, new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void shouldCount() {
        testInputTopic.pipeInput(null, "testing Kafka streams");
        testInputTopic.pipeInput(null, "testing kafka again");
        Assertions.assertThat(testOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("testing", 1L));
        Assertions.assertThat(testOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("kafka", 1L));
        Assertions.assertThat(testOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("streams", 1L));
        Assertions.assertThat(testOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("testing", 2L));
        Assertions.assertThat(testOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("kafka", 2L));
        Assertions.assertThat(testOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("again", 1L));
        Assertions.assertThat(testOutputTopic.isEmpty()).isTrue();
    }

    @AfterEach
    public void closeDriver() {
        testDriver.close();
    }
}
