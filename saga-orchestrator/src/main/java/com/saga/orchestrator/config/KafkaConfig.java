package com.saga.orchestrator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.shared.kafka.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * Declares all Kafka topics used across the saga and configures the consumer factory
 * with a JavaTimeModule-aware ObjectMapper so LocalDateTime fields deserialize correctly.
 */
@Configuration
public class KafkaConfig {

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(KafkaProperties kafkaProperties,
                                                           ObjectMapper objectMapper) {
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>(Object.class, objectMapper);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeHeaders(true);
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(deserializer));
    }

    // Retries transient failures with exponential backoff, then publishes to <topic>.DLT
    // instead of blocking the partition forever on a message that keeps failing.
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500L);
        backOff.setMultiplier(2.0);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory, DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    // ─── Order ───────────────────────────────────────────────────────────────
    @Bean public NewTopic orderCreateRequest() { return topic(KafkaTopics.ORDER_CREATE_REQUEST); }
    @Bean public NewTopic orderCreated()   { return topic(KafkaTopics.ORDER_CREATED); }
    @Bean public NewTopic orderConfirmed() { return topic(KafkaTopics.ORDER_CONFIRMED); }
    @Bean public NewTopic orderCancelled() { return topic(KafkaTopics.ORDER_CANCELLED); }

    // ─── Stock ───────────────────────────────────────────────────────────────
    @Bean public NewTopic stockReserveRequest()    { return topic(KafkaTopics.STOCK_RESERVE_REQUEST); }
    @Bean public NewTopic stockReserved()          { return topic(KafkaTopics.STOCK_RESERVED); }
    @Bean public NewTopic stockReservationFailed() { return topic(KafkaTopics.STOCK_RESERVATION_FAILED); }
    @Bean public NewTopic stockReleaseRequest()    { return topic(KafkaTopics.STOCK_RELEASE_REQUEST); }
    @Bean public NewTopic stockReleased()          { return topic(KafkaTopics.STOCK_RELEASED); }

    // ─── Payment ─────────────────────────────────────────────────────────────
    @Bean public NewTopic paymentProcessRequest() { return topic(KafkaTopics.PAYMENT_PROCESS_REQUEST); }
    @Bean public NewTopic paymentProcessed()      { return topic(KafkaTopics.PAYMENT_PROCESSED); }
    @Bean public NewTopic paymentFailed()         { return topic(KafkaTopics.PAYMENT_FAILED); }
    @Bean public NewTopic paymentCancelRequest()  { return topic(KafkaTopics.PAYMENT_CANCEL_REQUEST); }
    @Bean public NewTopic paymentCancelled()      { return topic(KafkaTopics.PAYMENT_CANCELLED); }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }
}
