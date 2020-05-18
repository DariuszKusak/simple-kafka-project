package com.kusak.dariusz.simplekafkaproject.config;

import com.kusak.dariusz.simplekafkaproject.model.SimpleModel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${json.deserializer.trusted.package}")
    private String JSON_DESERIALIZER_TRUSTED_PACKAGE;

    @Value("${kafka.bootstrap.server.config.localhost}")
    private String KAFKA_BOOTSTRAP_SERVER_CONFIG;

    @Value("${kafka.group.id}")
    private String KAFKA_GROUP_ID;

    @Bean
    public ProducerFactory<String, SimpleModel> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_CONFIG);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, SimpleModel> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, SimpleModel> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_CONFIG);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID);
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), createJsonDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SimpleModel> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SimpleModel> concurrentKafkaListenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
        return concurrentKafkaListenerContainerFactory;
    }

    @Bean
    public JsonDeserializer<SimpleModel> createJsonDeserializer() {
        JsonDeserializer<SimpleModel> deserializer = new JsonDeserializer<>(SimpleModel.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages(JSON_DESERIALIZER_TRUSTED_PACKAGE);
        deserializer.setUseTypeMapperForKey(true);
        return deserializer;
    }


}
