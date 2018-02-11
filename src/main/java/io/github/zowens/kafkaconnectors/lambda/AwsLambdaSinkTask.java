package io.github.zowens.kafkaconnectors.lambda;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.google.common.collect.ImmutableMap;

import net.jodah.failsafe.RetryPolicy;

public class AwsLambdaSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AwsLambdaSinkTask.class);

    private final AWSLambdaAsync lambdaClient;
    private String functionName;

    private RetryPolicy retryPolicy;

    private Map<TopicPartition, PartitionPublisher> publishers = ImmutableMap.of();

    public AwsLambdaSinkTask() {
        this(AWSLambdaAsyncClientBuilder.defaultClient());
    }

    public AwsLambdaSinkTask(AWSLambdaAsync lambdaClient) {
        this.lambdaClient = lambdaClient;
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting task: {}", props);

        this.functionName = props.get(Constants.CONFIG_KEY_LAMBDA_NAME);

        long maxDelay = Constants.CONFIG_RETRY_MAX_DELAY_DEFAULT;
        if (props.containsKey(Constants.CONFIG_KEY_RETRY_MAX_DELAY)) {
            maxDelay = Long.parseLong(props.get(Constants.CONFIG_KEY_RETRY_MAX_DELAY));
        }

        long delay = Constants.CONFIG_RETRY_DELAY_DEFAULT;
        if (props.containsKey(Constants.CONFIG_KEY_RETRY_DELAY)) {
            delay = Long.parseLong(props.get(Constants.CONFIG_KEY_RETRY_DELAY));
        }

        double jitter = Constants.CONFIG_RETRY_JITTER_DEFAULT;
        if (props.containsKey(Constants.CONFIG_KEY_RETRY_JITTER)) {
            jitter = Double.parseDouble(props.get(Constants.CONFIG_KEY_RETRY_JITTER));
        }

        this.retryPolicy = new RetryPolicy()
            .withBackoff(delay, maxDelay, TimeUnit.MILLISECONDS);

        if (jitter > 0 && jitter <= 1) {
            this.retryPolicy = this.retryPolicy.withJitter(jitter);
        }

        logger.info("Starting task with functionName={}", functionName);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        logger.debug("PUT RECORDS={}", records.size());
        if (records.isEmpty()) {
            return;
        }

        // queue the records
        //
        // TODO: are these always guaranteed to be in one partition?
        Set<TopicPartition> queuedPartitions = new HashSet<>();
        for (SinkRecord record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            this.publishers.get(tp).queue(record);

            logger.debug("Queueing record topic={}, partition={}, offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset());

            queuedPartitions.add(tp);
        }

        queuedPartitions.forEach(tp -> this.publishers.get(tp).trySend(true));
    }

    @Override
    public void stop() {
        publishers.forEach((tp, publisher) -> publisher.stop());
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        logger.info("Open Task: {}", partitions);

        final Map<TopicPartition, PartitionPublisher> newPublishers = new HashMap<>();
        for (TopicPartition topicPartition : partitions) {
            // try to copy the previous topic publisher
            //
            // TODO: is this really necessary??
            if (this.publishers.containsKey(topicPartition)) {
                newPublishers.put(topicPartition, this.publishers.get(topicPartition));
            } else {
                newPublishers.put(
                    topicPartition,
                    new PartitionPublisher(
                        topicPartition,
                        functionName,
                        lambdaClient,
                        retryPolicy));
            }
        }

        // stop the publishers not in the new task configuration
        this.publishers.forEach((tp, publisher) -> {
            if (!newPublishers.containsKey(tp)) {
                publisher.stop();
            }
        });

        this.publishers = newPublishers;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        logger.debug("Flushing publishers, size={}, currentOffers={}", this.publishers.size(), currentOffsets);
        this.publishers.forEach((tp, publisher) -> publisher.flush());
        logger.debug("Done flushing publishers");
    }
}
