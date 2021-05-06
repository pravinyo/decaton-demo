package com.example.utils;

import com.example.utils.IgnoreKeyList;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

import java.util.List;

@RefreshScope
@Component
public class ConfigProperties {

    @Value("${message.bootstrap_server}")
    private String bootstrapServers;

    @Value("${message.group_id}")
    private String groupId;

    @Value("${message.subscription_id}")
    private String subscriptionId;

    @Value("${message.topic}")
    private String topic;

    @Value("${message.per.partition.processing_rate}")
    private long messageProcessingRate;

    @Value("${message.per.partition.concurrent_thread_count}")
    private int messageConcurrency;

    @Value("${message.max_pending_record}")
    private int maxPendingRecords;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getTopic() {
        return topic;
    }

    public long getMessageProcessingRate() {
        return messageProcessingRate;
    }

    public int getMessageConcurrency() {
        return messageConcurrency;
    }

    public int getMaxPendingRecords() {
        return maxPendingRecords;
    }

    public List<String> getKeysToIgnore(){
        return IgnoreKeyList.getKeysToIgnore();
    }

    public Object getProperty(PropertyDefinition<?> propertyDefinition){
        String name = propertyDefinition.name();

        if (name.equals(ProcessorProperties.CONFIG_IGNORE_KEYS.name())){
            return getKeysToIgnore();
        }else if (name.equals(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY.name())){
            return getMessageConcurrency();
        }else if(name.equals(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS.name())){
            return getMaxPendingRecords();
        }else if (name.equals(ProcessorProperties.CONFIG_PROCESSING_RATE.name())){
            return getMessageProcessingRate();
        }else {
            throw new IllegalArgumentException("no such property definition: " + name);
        }
    }

    @Override
    public String toString() {
        return "ConfigProperties{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", groupId='" + groupId + '\'' +
                ", subscriptionId='" + subscriptionId + '\'' +
                ", topic='" + topic + '\'' +
                ", messageProcessingRate=" + messageProcessingRate +
                ", messageConcurrency=" + messageConcurrency +
                ", maxPendingRecords=" + maxPendingRecords +
                '}';
    }
}
