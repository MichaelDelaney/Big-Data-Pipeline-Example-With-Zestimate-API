package com.bigdataprocessing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.IOException;
import java.util.*;
import static com.bigdataprocessing.Persistence.persistRecord;

/**
 * Kafka client to consume the data of the flume destination.
 *  Flume is configured to use a kafka sink.
 */
public class Consumer {

    public static void main(String[] args) throws IOException {

        //Kafka consumer properties
        String topic ="ZillowAppTopic";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "zillow.consumer");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("batch.size", 20);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        // Polls for records and passes them to persistence service
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (ConsumerRecord<String, String> record : records) {
                persistRecord(record);
            }
        }
    }

}
