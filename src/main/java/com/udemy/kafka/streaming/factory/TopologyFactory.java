package com.udemy.kafka.streaming.factory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.udemy.kafka.streaming.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import static com.udemy.kafka.streaming.Topics.*;

public class TopologyFactory {

    private static final List<String> ALLOWED_COLOURS = Arrays.asList("green","red","blue");

    public static Topology getWordsCountTopology(){
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.<String, String>stream(Topics.WORDS_COUNT_SOURCE)
//                    .print(Printed.toSysOut());
                    .peek((key, value) -> System.out.println("Input >>>>>>" + key + ":" + value))
                    .mapValues(value -> value.toLowerCase())
                    .flatMapValues(value -> Arrays.asList(value.split(" ")))
                    .selectKey((key, value) -> value)
                    .groupByKey()
                    .count()
                    .toStream()
                    .peek((key, value) -> System.out.println("output>>>>" + key + ":" + value))
                    .to(WORDS_COUNT_SINK, Produced.with(Serdes.String(), Serdes.Long()));
        return streamsBuilder.build();
    }

    public static Topology getFavoriteColourTopology(){
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.table(FAVORITE_COLOUR_SOURCE, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(v -> v.replace(" ","").toLowerCase())
                .filter((key, value) -> ALLOWED_COLOURS.contains(value))
                .groupBy((key, value) -> KeyValue.pair(value, value))
                .count(Named.as("Counts"))
                .toStream()
                .peek((key, value) -> System.out.println("output >>>>>>" + key + ":" + value))
                .to(FAVORITE_COLOURS_SINK);
        return streamsBuilder.build();
    }

    public static Topology getBankBalanceTopology(){
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.<String, String>stream("bank-transactions")
                .mapValues((readOnlyKey, value) -> {
                    try {
                        return new ObjectMapper().readTree(value).asText("amount");
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .groupByKey()
                .reduce((value1, value2) -> value1 + value2)
                .toStream()
                .to("bank-balances");
        return streamsBuilder.build();
    }
}
