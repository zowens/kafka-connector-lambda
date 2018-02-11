package io.github.zowens.kafkaconnectors.lambda;

import java.util.Base64;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.util.concurrent.Scheduler;
import net.jodah.failsafe.util.concurrent.Schedulers;

class PartitionPublisher {
    private static final Logger logger = LoggerFactory.getLogger(PartitionPublisher.class);

    private static final String EVENT_PREFIX = "{\"Records\":[";
    private static final String EVENT_SUFFIX = "]}";
    private static final int SAFE_APPEND_LEN = Constants.LAMBDA_MAX_BYTES - EVENT_SUFFIX.length();


    private static final Scheduler retryScheduler = Schedulers.of(new ScheduledThreadPoolExecutor(3));

    private final RetryPolicy retryPolicy;
    private final AWSLambdaAsync lambdaClient;
    private final String functionName;
    private final String context;

    private final Queue<String> queuedRecords;
    private CompletableFuture<Object> currentSend = null;

    public PartitionPublisher(
        TopicPartition topicPartition,
        String functionName,
        AWSLambdaAsync lambdaClient,
        RetryPolicy retryPolicy
    ) {
        this.queuedRecords = new LinkedList<>();
        this.functionName = functionName;
        this.lambdaClient = lambdaClient;
        this.retryPolicy = new RetryPolicy(retryPolicy)
            // force forever retries
            .withMaxRetries(Integer.MAX_VALUE);
            // TODO: pause stream on retry?
            // http://jodah.net/failsafe/javadoc/net/jodah/failsafe/Listeners.html

        // create the context passed to each event
        try {

            this.context = Base64.getEncoder().encodeToString(
                Constants.OBJECT_MAPPER.writeValueAsBytes(
                    new ImmutableMap.Builder<String, Object>()
                        .put("topic", topicPartition.topic())
                        .put("partition", topicPartition.partition())
                        .build()));
        } catch (JsonProcessingException e) {
            logger.error("Error serializing JSON for context", e);
            throw Throwables.propagate(e);
        }
    }

    public synchronized void queue(SinkRecord record) {
        final Record rec = new Record();
        rec.setKey(record.key());
        rec.setValue(record.value());
        rec.setOffset(record.kafkaOffset());
        try {
            this.queuedRecords.add(Constants.OBJECT_MAPPER.writeValueAsString(rec));
        } catch (JsonProcessingException e) {
            logger.error("Error serializing JSON for record", e);
            throw Throwables.propagate(e);
        }
    }

    public synchronized void trySend(boolean drain) {
        if (currentSend != null && currentSend.isDone()) {
            currentSend = null;
        }

        if (queuedRecords.isEmpty() || currentSend != null) {
            logger.trace("No need to send empty={}, sending={}",
                    queuedRecords.isEmpty(), currentSend != null);
            return;
        }

        // form the payload
        final StringBuilder sb = new StringBuilder(EVENT_PREFIX);
        while (!queuedRecords.isEmpty() &&
               (sb.length() + queuedRecords.peek().length() + 1) < SAFE_APPEND_LEN) {

            // add the comma delimiter
            if (sb.length() > EVENT_PREFIX.length()) {
                sb.append(',');
            }

            sb.append(queuedRecords.poll());
        }

        sb.append(EVENT_SUFFIX);

        currentSend = executeLambda(sb.toString());

        // try to send again if there are records, and we want to drain
        if (drain) {
            currentSend.handle((res, e) -> {
                if (e == null) {
                    this.trySend(true);
                }
                return null;
            });
        }
    }

    public void flush() {
        while (currentSend != null && !currentSend.isDone()) {
            try {
                logger.info("Waiting for future to flush");
                currentSend.get();
                currentSend = null;
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Unable to flush", e);
                throw Throwables.propagate(e);
            }
            trySend(false);
        }
    }

    public void stop() {
        if (this.currentSend != null && !this.currentSend.isDone() && !this.currentSend.isCancelled()) {
            this.currentSend.cancel(true);
        }
        queuedRecords.clear();
    }

    @VisibleForTesting
    int queuedRecords() {
        return this.queuedRecords.size();
    }

    private CompletableFuture<Object> executeLambda(String payload) {
        return Failsafe.with(retryPolicy).with(retryScheduler).future(() -> {
            InvokeRequest request = new InvokeRequest()
                .withFunctionName(functionName)
                .withInvocationType(InvocationType.RequestResponse)
                .withLogType(LogType.None)
                .withClientContext(context)
                .withPayload(payload);

            logger.debug("Starting Lambda invoke");
            final CompletableFuture<Object> future = new CompletableFuture<>();
            this.lambdaClient.invokeAsync(request, new AsyncHandler<InvokeRequest, InvokeResult>() {
                @Override
                public void onError(Exception exception) {
                    logger.error("Error invoking Lambda {}", functionName, exception);
                    future.completeExceptionally(exception);
                }

                @Override
                public void onSuccess(InvokeRequest request, InvokeResult result) {
                    if (result.getFunctionError() == null) {
                        logger.debug("Lambda success: {}", result);
                        future.complete(new Object());
                    } else {
                        logger.warn("Lambda failure");
                        future.completeExceptionally(new LambdaFailureException());
                    }
                }
            });
            return future;
        });
    }

    public static class LambdaFailureException extends RuntimeException {
        private static final long serialVersionUID = -6894150638125317048L;
    }
}
