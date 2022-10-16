package com.kafkaproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproducer.model.User;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UserProducerUnitTest {

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    UserProducer userProducer;

    @Test
    void sendUser_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        User user = User.builder()
                .id(null)
                .fullname("Elmehdi ASSIMEDDINE")
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception calling kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // When
        assertThrows(Exception.class, () -> userProducer.sendUser(user).get());

        // Then
    }

    @Test
    void sendUser_success() throws JsonProcessingException, ExecutionException, InterruptedException {
        // Given
        User user = User.builder()
                .id(null)
                .fullname("Elmehdi ASSIMEDDINE")
                .build();

        String record = objectMapper.writeValueAsString(user);

        SettableListenableFuture future = new SettableListenableFuture();
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("user-topic", user.getId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic-user", 1), 1, 1, 342, System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord, recordMetadata);

        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // When
       ListenableFuture<SendResult<Integer, String>> listenableFuture = userProducer.sendUser(user);

        // Then
        SendResult<Integer, String> sendResult1 = listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition() == 1;
    }
}
