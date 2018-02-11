package io.github.zowens.kafkaconnectors.lambda;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.google.common.collect.ImmutableMap;

public class AwsLambdaSinkConnector extends SinkConnector {
    private ImmutableMap<String, String> config;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = ImmutableMap.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AwsLambdaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return IntStream.range(0, maxTasks)
            .mapToObj(i -> ImmutableMap.copyOf(config))
            .collect(Collectors.toList());
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(
                Constants.CONFIG_KEY_LAMBDA_NAME,
                Type.STRING,
                null,
                Importance.HIGH,
                "Name of the Lambda function")
            .define(
                Constants.CONFIG_KEY_RETRY_DELAY,
                Type.LONG,
                Constants.CONFIG_RETRY_DELAY_DEFAULT,
                Importance.LOW,
                "Starting delay between failed Lambda execution retries")
            .define(
                Constants.CONFIG_KEY_RETRY_MAX_DELAY,
                Type.LONG,
                Constants.CONFIG_RETRY_MAX_DELAY_DEFAULT,
                Importance.LOW,
                "Maximum delay between failed Lambda execution retries")
            .define(
                Constants.CONFIG_KEY_RETRY_JITTER,
                Type.DOUBLE,
                Constants.CONFIG_RETRY_JITTER_DEFAULT,
                Importance.LOW,
                "Jitter to apply to the exponential backoff between failed Lambda execution retries");
    }

}
