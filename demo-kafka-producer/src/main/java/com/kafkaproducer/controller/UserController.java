package com.kafkaproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaproducer.model.User;
import com.kafkaproducer.producer.UserProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class UserController {

    @Autowired
    UserProducer userProducer;

    @PostMapping("/api/users")
    public ResponseEntity<User> postUser(@RequestBody User user) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        userProducer.sendUser(user);
        return ResponseEntity.status(HttpStatus.CREATED).body(user);
    }

}
