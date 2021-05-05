package com.example.processor;

import com.example.protocol.Tasks.HelloTask;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.processors.CompactionProcessor;
import com.linecorp.decaton.processor.runtime.*;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

import static com.linecorp.decaton.processor.processors.CompactionProcessor.CompactChoice;

@RefreshScope
@Configuration
@Component
public class ProcessorConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorConfig.class);

    @Value("${message.bootstrap_server}")
    private String BOOTSTRAP_SERVERS;

    private static final String CLIENT_ID = "decaton-processor";

    @Value("${message.group_id}")
    private String GROUP_ID;

    @Value("${message.subscription_id}")
    private String SUBSCRIPTION_ID;

    @Value("${message.topic}")
    private String TOPIC;

    @Value("${message.per.partition.processing_rate}")
    private long DECATON_MESSAGE_PROCESSING_RATE;

    @Value("${message.per.partition.concurrent_thread_count}")
    private int DECATON_MESSAGE_CONCURRENT_THREAD_COUNT;

    @Value("${message.max_pending_record}")
    private int DECATON_MAX_PENDING_RECORD;


    /*
    @Bean
    public ProcessorSubscription helloProcessorSubscription(HelloTaskProcessor processor) {
        return newProcessorSubscription(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
    }

    @Bean
    public ProcessorSubscription retryProcessorSubscription(HelloTaskProcessorRetrySync processor) {
        return newProcessorSubscriptionWithRetry(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
    }

    @Bean
    public ProcessorSubscription retryAsyncProcessorSubscription(HelloTaskProcessorRetryASync processor) {
        return newProcessorSubscriptionWithRetry(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
    }

    @Bean
    public ProcessorSubscription rateLimitProcessorSubscription(HelloTaskProcessor processor) {
        return newProcessorSubscriptionWithRateLimiting(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
    }

    @Bean
    public ProcessorSubscription compactionProcessorSubscription(HelloTaskProcessor processor) {
        return newProcessorSubscriptionWithCompaction(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
    }
    */

    @Bean
    public ProcessorSubscription ignoreKeysProcessorSubscription(HelloTaskProcessor processor) {
        LOGGER.info(toString());
        return newProcessorSubscriptionWithKeyIgnore(SUBSCRIPTION_ID, TOPIC, HelloTask.parser(), processor);
    }

    private Properties propertyConfig() {
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return config;
    }

    private <T extends GeneratedMessageV3> ProcessorSubscription newProcessorSubscription(
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

    private <T extends GeneratedMessageV3> ProcessorSubscription newProcessorSubscriptionWithRetry(
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

    private <T extends GeneratedMessageV3> ProcessorSubscription newProcessorSubscriptionWithRateLimiting(
            String subscriptionId, String topic, Parser<T> parser, DecatonProcessor<T> processor) {

        ProcessorsBuilder<T> processorsBuilder =
                ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(parser))
                        .thenProcess(processor);

        PropertySupplier propertySupplier =
                StaticPropertySupplier.of(
                        // When the processing rate reaches configured rate limit, rate limiter
                        // will slow down the processing rather than discarding tasks.
                        // Decaton’s rate limiter allows sudden increase in traffic.
                        //
                        //0 => Stop all processing but the task currently being processed isn’t interrupted
                        //
                        //-1 (default) => Unlimited
                        //
                        //Any positive number => Do the best to process tasks with the given value per second.
                        // Note that the actual processing rate may not reach the given value if a task takes
                        // over a second to process or the value is greater than actual throughput per second.
                        Property.ofStatic(ProcessorProperties.CONFIG_PROCESSING_RATE, 2L),
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 10),
                        Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 20));

        return SubscriptionBuilder.newBuilder(subscriptionId)
                .processorsBuilder(processorsBuilder)
                .consumerConfig(propertyConfig())
                .properties(propertySupplier)
                .buildAndStart();
    }

    private ProcessorSubscription newProcessorSubscriptionWithCompaction(
            String subscriptionId, String topic, Parser<HelloTask> parser, DecatonProcessor<HelloTask> processor) {

        ProcessorsBuilder<HelloTask> processorsBuilder =
                ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(parser))
                        .thenProcess(createCompactionProcessor())
                        .thenProcess(processor);

        PropertySupplier propertySupplier =
                StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 2),
                        Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 100));

        return SubscriptionBuilder.newBuilder(subscriptionId)
                .processorsBuilder(processorsBuilder)
                .consumerConfig(propertyConfig())
                .properties(propertySupplier)
                .buildAndStart();
    }

    private static CompactionProcessor<HelloTask> createCompactionProcessor() {
        return new CompactionProcessor<>(1000L, (left, right) -> {
            if (left.task().getCreatedAt().getNanos() == right.task().getCreatedAt().getNanos()) {
                return CompactChoice.PICK_EITHER;
            } else if (left.task().getCreatedAt().getNanos() > right.task().getCreatedAt().getNanos()) {
                return CompactChoice.PICK_LEFT;
            } else {
                return CompactChoice.PICK_RIGHT;
            }
        });
    }

    private <T extends GeneratedMessageV3> ProcessorSubscription newProcessorSubscriptionWithKeyIgnore(
            String subscriptionId, String topic, Parser<T> parser, DecatonProcessor<T> processor) {

        ProcessorsBuilder<T> processorsBuilder =
                ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(parser))
                        .thenProcess(processor);

        PropertySupplier propertySupplier =
                StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_IGNORE_KEYS, IgnoreKeyList.getKeysToIgnore()),
                        Property.ofStatic(ProcessorProperties.CONFIG_PROCESSING_RATE, DECATON_MESSAGE_PROCESSING_RATE),
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, DECATON_MESSAGE_CONCURRENT_THREAD_COUNT),
                        Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, DECATON_MAX_PENDING_RECORD));

        return SubscriptionBuilder.newBuilder(subscriptionId)
                .processorsBuilder(processorsBuilder)
                .consumerConfig(propertyConfig())
                .properties(propertySupplier)
                .buildAndStart();
    }

    @Override
    public String toString() {
        return "ProcessorConfig{" +
                "BOOTSTRAP_SERVERS='" + BOOTSTRAP_SERVERS + '\'' +
                ", GROUP_ID='" + GROUP_ID + '\'' +
                ", SUBSCRIPTION_ID='" + SUBSCRIPTION_ID + '\'' +
                ", TOPIC='" + TOPIC + '\'' +
                ", DECATON_MESSAGE_PROCESSING_RATE=" + DECATON_MESSAGE_PROCESSING_RATE +
                ", DECATON_MESSAGE_CONCURRENT_THREAD_COUNT=" + DECATON_MESSAGE_CONCURRENT_THREAD_COUNT +
                ", DECATON_MAX_PENDING_RECORD=" + DECATON_MAX_PENDING_RECORD +
                '}';
    }
}
