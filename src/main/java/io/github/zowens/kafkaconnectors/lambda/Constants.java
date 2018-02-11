package io.github.zowens.kafkaconnectors.lambda;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

public class Constants {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new GuavaModule());

    public static int LAMBDA_MAX_BYTES = 6000000;

    public static String CONFIG_KEY_LAMBDA_NAME = "connect.lambda.function.name";

    public static String CONFIG_KEY_RETRY_DELAY = "connect.lambda.retry.delay.millis";
    public static long CONFIG_RETRY_DELAY_DEFAULT = 100;

    public static String CONFIG_KEY_RETRY_MAX_DELAY = "connect.lambda.retry.delay.max.millis";
    public static long CONFIG_RETRY_MAX_DELAY_DEFAULT = 1000;

    public static String CONFIG_KEY_RETRY_JITTER = "connect.lambda.retry.jitter";
    public static double CONFIG_RETRY_JITTER_DEFAULT = 0.0d;
}
