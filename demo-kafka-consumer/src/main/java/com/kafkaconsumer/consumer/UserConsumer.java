package com.kafkaconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaconsumer.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UserConsumer {

    @Autowired
    private UserService userService;

    @KafkaListener(topics= {"user-topic"}, groupId = "user-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord : {} ", consumerRecord);

        System.out.println("Step 1");

        userService.processUser(consumerRecord);
    }

}
