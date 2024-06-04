package com.udemy.kafka.streaming.producers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.udemy.kafka.streaming.builder.KafkaPropertiesBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.time.LocalDateTime;
import java.util.Random;

public class RandomTransactionsProducer {

    public void sendTransactionsBatch() {
        final KafkaProducer<String, String> transactionsProducer = new KafkaProducer(new KafkaPropertiesBuilder()
                .withApplicationId("random-transaction-producer")
                .withProducerIdempotentEnabled("true")
                .build(), Serdes.String().serializer(), Serdes.String().serializer()
        );

        while (true) {
            try {
                transactionsProducer.send(createRandomTransaction("john"));
                Thread.sleep(100);
                transactionsProducer.send(createRandomTransaction("jane"));
                Thread.sleep(100);
                transactionsProducer.send(createRandomTransaction("johnjane"));
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
        transactionsProducer.close();
    }

    public ProducerRecord<String, String> createRandomTransaction(String name) {
        final ObjectNode transactionNode = new ObjectMapper().createObjectNode();
        transactionNode.put("name", name);
        transactionNode.put("amount", new Random(0).nextInt(100));
        transactionNode.put("time", LocalDateTime.now().toString());
        return new ProducerRecord<>("bank-transactions", name, transactionNode.toString());
    }
}
