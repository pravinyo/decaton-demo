package com.example.processor;

import com.example.protocol.Tasks;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@Component
public class HelloTaskProcessorRetryASync implements DecatonProcessor<Tasks.HelloTask> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloTaskProcessorRetryASync.class);

    @Override
    public void process(ProcessingContext<Tasks.HelloTask> context, Tasks.HelloTask task) throws InterruptedException {
        // manually handling completion of task
        DeferredCompletion completion = context.deferCompletion();

        // call to 3rd party verification and get token
        CompletableFuture<String> tokenFuture = verifyDetails(task);
        tokenFuture.whenComplete((token, exception) -> {
            if (!token.isEmpty()){
                // call to payment server
                completeThePayment(token).whenComplete((result, error) -> {
                   // reply the response to the user
                   // we are done with the execution
                    LOGGER.info("Payment done for task {}", task.getMessage());
                   completion.complete(); // mark the task as completed so that kafka can commit this offset
                });
            }else if (exception != null){
                try {
                    LOGGER.info("Verification server is busy, push to retry");
                    if (context.metadata().retryCount()>10){
                        LOGGER.info("Could not validate task, server is busy");
                    }else {
                        LOGGER.info("Server is busy, retrying: {}", context.metadata().retryCount());
                        context.retry();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private CompletableFuture<String> verifyDetails(Tasks.HelloTask task) throws InterruptedException {
        LOGGER.info("Verifying details");
        LOGGER.info("Task: {}",task.getMessage());
        Thread.sleep(100);
        long millis = System.currentTimeMillis();

        if (millis % 2 ==0 ){
            return CompletableFuture.failedFuture(new IOException("Verification server is busy"));
        }
        return CompletableFuture.completedFuture("123IOQE");
    }

    private CompletableFuture<String> completeThePayment(String token){
        return CompletableFuture.completedFuture("Success");
    }
}
