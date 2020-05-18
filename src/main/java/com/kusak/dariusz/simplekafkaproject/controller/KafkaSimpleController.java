package com.kusak.dariusz.simplekafkaproject.controller;

import com.kusak.dariusz.simplekafkaproject.model.SimpleModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaSimpleController {

    @Value("${kafka.topic.message}")
    private String MY_TOPIC;

    private KafkaTemplate<String, SimpleModel> kafkaTemplate;

    public KafkaSimpleController(KafkaTemplate<String, SimpleModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public void post(@RequestBody SimpleModel simpleModel) {
        kafkaTemplate.send(MY_TOPIC, simpleModel);
    }

    @KafkaListener(topics = "${kafka.topic.message}")
    public void getFromKafka(SimpleModel simpleModel) {
        System.out.println(simpleModel.toString());
    }


}
