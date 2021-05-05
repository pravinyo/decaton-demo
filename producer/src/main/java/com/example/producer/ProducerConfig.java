package com.example.producer;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.protocol.Tasks.HelloTask;
import com.google.protobuf.MessageLite;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.protobuf.ProtocolBuffersSerializer;
import org.springframework.stereotype.Component;

@RefreshScope
@Component
@Configuration
public class ProducerConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConfig.class);

    @Value("${message.bootstrap_server}")
    private String BOOTSTRAP_SERVERS;

    private static final String CLIENT_ID = "decaton-client";

    @Value("${message.application_id}")
    private String APPLICATION_ID;

    @Value("${message.topic}")
    private String TOPIC;

    @Bean
    public DecatonClient<HelloTask> helloClient() {
        LOGGER.info(toString());
        return newClient(TOPIC);
    }

    private <T extends MessageLite> DecatonClient<T> newClient(String topic) {
        Properties config = new Properties();
        config.setProperty(CLIENT_ID_CONFIG, CLIENT_ID);
        config.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        return DecatonClient.producing(topic, new ProtocolBuffersSerializer<T>())
                            .applicationId(APPLICATION_ID)
                            .producerConfig(config)
                            .build();
    }

    @Override
    public String toString() {
        return "ProducerConfig{" +
                "BOOTSTRAP_SERVERS='" + BOOTSTRAP_SERVERS + '\'' +
                ", APPLICATION_ID='" + APPLICATION_ID + '\'' +
                ", TOPIC='" + TOPIC + '\'' +
                '}';
    }
}
