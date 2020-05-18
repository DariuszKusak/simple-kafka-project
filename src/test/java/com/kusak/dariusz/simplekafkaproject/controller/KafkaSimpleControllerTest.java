package com.kusak.dariusz.simplekafkaproject.controller;


import com.kusak.dariusz.simplekafkaproject.model.SimpleModel;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

@EmbeddedKafka(partitions = 1, topics = "${kafka.topic.message}")
@SpringBootTest
class KafkaTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    JsonDeserializer jsonDeserializer;

    @Value("${kafka.topic.message}")
    String TEST_TOPIC;

    private final static String KEY_TEST = "KET_TEST";

    @Test
    void testSingleMessage() {
        SimpleModel simpleModelMessage = createSimpleModelMessage1();

        Producer<String, SimpleModel> producer = configureProducer();
        producer.send(new ProducerRecord<>(TEST_TOPIC, KEY_TEST, simpleModelMessage));
        producer.close();

        Consumer<String, SimpleModel> consumer = configureConsumer();
        ConsumerRecord<String, SimpleModel> resultRecord = KafkaTestUtils.getSingleRecord(consumer, TEST_TOPIC);
        consumer.close();

        assertEquals(KEY_TEST, resultRecord.key());
        assertEquals(TEST_TOPIC, resultRecord.topic());
        assertEquals(simpleModelMessage.getField1(), resultRecord.value().getField1());
        assertEquals(simpleModelMessage.getField2(), resultRecord.value().getField2());
    }


    @Test
    void testPluralMessages() {
        List<SimpleModel> simpleModelList = List.of(createSimpleModelMessage1(), createSimpleModelMessage2(), createSimpleModelMessage3());

        Producer<String, SimpleModel> producer = configureProducer();
        for (SimpleModel simpleModel : simpleModelList) {
            producer.send(new ProducerRecord<>(TEST_TOPIC, KEY_TEST, simpleModel));
        }
        producer.close();

        Consumer<String, SimpleModel> consumer = configureConsumer();
        ConsumerRecords<String, SimpleModel> resultRecords = KafkaTestUtils.getRecords(consumer, 1000);
        Iterable<ConsumerRecord<String, SimpleModel>> records = resultRecords.records(TEST_TOPIC);
        List<SimpleModel> resultMessages = new ArrayList<>();
        records.forEach(r -> resultMessages.add(r.value()));
        consumer.close();
        assertThat(resultMessages, hasSize(simpleModelList.size()));
        assertEquals(simpleModelList.get(0).getField1(), resultMessages.get(0).getField1());
        assertEquals(simpleModelList.get(1).getField2(), resultMessages.get(1).getField2());
        assertEquals(simpleModelList.get(2).getField1(), resultMessages.get(2).getField1());
    }

    private Consumer<String, SimpleModel> configureConsumer() {
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        Consumer<String, SimpleModel> consumer =
                new DefaultKafkaConsumerFactory<String, SimpleModel>(consumerProps, new StringDeserializer(), jsonDeserializer)
                        .createConsumer();
        consumer.subscribe(Collections.singleton(TEST_TOPIC));
        return consumer;
    }

    private Producer<String, SimpleModel> configureProducer() {
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<String, SimpleModel>(producerProps).createProducer();
    }

    private SimpleModel createSimpleModelMessage1() {
        return SimpleModel.builder()
                .field1("f1test1")
                .field2("f2test1")
                .build();
    }

    private SimpleModel createSimpleModelMessage2() {
        return SimpleModel.builder()
                .field1("f1test2")
                .field2("f2test2")
                .build();
    }

    private SimpleModel createSimpleModelMessage3() {
        return SimpleModel.builder()
                .field1("f1test3")
                .field2("f2test3")
                .build();
    }


}

