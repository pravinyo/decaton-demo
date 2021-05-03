package com.example.processor;

import java.time.Duration;
import java.util.Properties;

import com.linecorp.decaton.processor.runtime.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.protocol.Tasks.HelloTask;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;

@Configuration
public class ProcessorConfig {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "decaton-processor";
    private static final String GROUP_ID = "decaton-demo";
    private static final String SUBSCRIPTION_ID = "hello-processor";
    private static final String TOPIC = "topic1";

//    @Bean
//    public ProcessorSubscription helloProcessorSubscription(HelloTaskProcessor processor) {
//        return newProcessorSubscription(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
//    }

//    @Bean
//    public ProcessorSubscription retryProcessorSubscription(HelloTaskProcessorRetrySync processor) {
//        return newProcessorSubscriptionWithRetry(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
//    }

    @Bean
    public ProcessorSubscription retryAsyncProcessorSubscription(HelloTaskProcessorRetryASync processor) {
        return newProcessorSubscriptionWithRetry(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
    }

    private static <T extends GeneratedMessageV3> ProcessorSubscription newProcessorSubscription(
            String subscriptionId, String topic, Parser<T> parser, DecatonProcessor<T> processor) {

        ProcessorsBuilder<T> processorsBuilder =
                ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(parser))
                                 .thenProcess(processor);

        PropertySupplier propertySupplier =
                StaticPropertySupplier.of(
                        // Per Partition 10 consumer threads
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 10),
                        // Allow max pending records
                        Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 100));

        return SubscriptionBuilder.newBuilder(subscriptionId)
                                  .processorsBuilder(processorsBuilder)
                                  .consumerConfig(propertyConfig())
                                  .properties(propertySupplier)
                                  .buildAndStart();
    }

    private static <T extends GeneratedMessageV3> ProcessorSubscription newProcessorSubscriptionWithRetry(
            String subscriptionId, String topic, Parser<T> parser, DecatonProcessor<T> processor) {


        /* Decaton needs a retry topic for retry functionality. The name of the retry topic will be
           `topic name` + `-retry` (topic1-retry) by default. For example, if you have a topic called weather-task then
           you need to create a topic called weather-task-retry for retry function. You can also
           overwrite the default naming convention.
         */
        ProcessorsBuilder<T> processorsBuilder =
                ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(parser))
                        .thenProcess(processor);

        PropertySupplier propertySupplier =
                StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 10),
                        Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 100));

        return SubscriptionBuilder.newBuilder(subscriptionId)
                .enableRetry(RetryConfig.withBackoff(Duration.ofMillis(100)))
                .processorsBuilder(processorsBuilder)
                .consumerConfig(propertyConfig())
                .properties(propertySupplier)
                .buildAndStart();
    }

    private static Properties propertyConfig(){
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return config;
    }
}
