package com.udemy.kafka.streaming.producers;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.json.JSONException;
import org.json.JSONStringer;
import org.junit.jupiter.api.Test;

public class TransactionsProducerTest {

    private RandomTransactionsProducer transactionsProducer = new RandomTransactionsProducer();

    @Test
    public void createRandomTransaction() throws JSONException {
        final ProducerRecord<String, String> danilo = transactionsProducer.createRandomTransaction("danilo");
        Assertions.assertThat(danilo.value()).contains("{\"name\":\"danilo\",\"amount\":60,\"time\":");
    }
}