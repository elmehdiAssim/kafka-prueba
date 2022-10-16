package com.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaconsumer.model.User;
import com.kafkaconsumer.repository.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UserService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private UserRepository userRepository;

    public void processUser(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        System.out.println("Step 2");

        User user = objectMapper.readValue(consumerRecord.value(), User.class);
        log.info("User : {} ", user);
        validate(user);
        userRepository.save(user);
        log.info("Successfully Persisted the user {} ", user);
    }

    private void validate(User user) {
        if (user.getFullname() == null) {
            throw new IllegalArgumentException("fullname should not be null!");
        }

        if (user.getId() == 999) {
            System.out.println("user id : " + user.getId());
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }
    }

}
