package com.kafkaconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaconsumer.model.User;
import com.kafkaconsumer.repository.UserRepository;
import com.kafkaconsumer.service.UserService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"user-topic", "user-topic.RETRY", "user-topic.DLT"}, partitions = 1)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup:false"
})
public class UserConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    UserConsumer userConsumerSpy;

    @SpyBean
    UserService userServiceSpy;

    @Autowired
    UserRepository userRepository;

    private Consumer<Integer, String> consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @BeforeEach
    void setup() {

        var container = endpointRegistry.getListenerContainers()
                .stream().filter(messageListenerContainer ->
                        messageListenerContainer.getGroupId().equals("user-listener-group"))
                .collect(Collectors.toList()).get(0);

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        /*
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
         */

    }

    @AfterEach
    void tearDown() {
        userRepository.deleteAll();
    }

    @Test
    void publishNewUser() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"id\":null,\"fullname\":\"Elmehdi Assimeddine\"}";
        kafkaTemplate.sendDefault(json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(userConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(userServiceSpy, times(1)).processUser(isA(ConsumerRecord.class));

        List<User> userList = (List<User>) userRepository.findAll();
        assert userList.size() == 1;
        userList.forEach(user -> {
            assert user.getId() != null;
            assertEquals("Elmehdi Assimeddine", user.getFullname());
        });
    }

    @Test
    void publishNewUser_fullname_null() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"id\":null}";
        kafkaTemplate.sendDefault(json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // then
        verify(userConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(userServiceSpy, times(3)).processUser(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic);
        System.out.println("ConsumerRecord is: " + consumerRecord.value());
        assertEquals(json, consumerRecord.value());


    }

    @Test
    void publishNewUser_id_999() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"id\":999,\"fullname\":\"Elmehdi Assimeddine\"}";
        kafkaTemplate.sendDefault(json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // then
        verify(userConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(userServiceSpy, times(3)).processUser(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        System.out.println("ConsumerRecord is: " + consumerRecord.value());
        assertEquals(json, consumerRecord.value());

    }

}
