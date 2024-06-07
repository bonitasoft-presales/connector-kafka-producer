package com.bonitasoft;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class EventProducer {

    private static final Logger LOGGER = Logger.getLogger(EventProducer.class.getName());

    private Producer<Long, String> currentProducer;

    public Producer<Long, String> createProducer(String kafkaServer, String kafkaUser, String kafkaPassword) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        LOGGER.info("Producer created.");
        return new KafkaProducer<>(props);
    }

    public List<Future<RecordMetadata>> send(String topic, Long id, String message) {
        List<Future<RecordMetadata>> results = new ArrayList<Future<RecordMetadata>>();
        final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(
                topic,
                null,
                id,
                message,
                new ArrayList<>());
        LOGGER.info("Message has been sent.");
        results.add(currentProducer.send(record));
        return results;
    }

}