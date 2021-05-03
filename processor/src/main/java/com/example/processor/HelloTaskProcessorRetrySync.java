package com.example.processor;

import com.example.protocol.Tasks;
import com.google.protobuf.Timestamp;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

@Component
public class HelloTaskProcessorRetrySync implements DecatonProcessor<Tasks.HelloTask> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloTaskProcessorRetrySync.class);

    @Override
    public void process(ProcessingContext<Tasks.HelloTask> context, Tasks.HelloTask task) throws InterruptedException {

        try {
            Timestamp timestamp = task.getCreatedAt();
            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp.getSeconds(),
                    timestamp.getNanos()),
                    TimeZone.getDefault().toZoneId());
            if (timestamp.getSeconds() % 2 == 0){
                // to simulate 3rd party system is slow
                throw new IOException("Source system is not responding");
            }
            LOGGER.info("message={} createdAt={}", task.getMessage(), dateTime);
        }catch (Exception ex){
            LOGGER.info("Task cannot be completed with error {}",ex.getMessage());

            if (context.metadata().retryCount() > 5){
                /* Decaton will record how many time a task has been retried. You can use this method
                   to get the number of counts.
                 */
                LOGGER.error("Task retied for {} times but action cannot be completed",context.metadata().retryCount());
            }else {
                context.retry(); // Decaton will handle remaining works for you.
                LOGGER.info("Task({}) is scheduled for retry",task.getMessage());
            }
        }
    }
}
