#!/bin/sh

TOPIC=lambdatest

key="$1"

case $1 in
    produce)
        docker-compose exec broker \
            bash -c "echo '{\"schema\": {\"type\": \"string\"}, \"payload\": \"foo\"}|{\"schema\": {\"type\": \"string\"},\"payload\": \"baz\"}' | kafka-console-producer --topic $TOPIC --property parse.key=true --property key.separator='|' --request-required-acks 1 --broker-list broker:9092"
    ;;
    consume)
        docker-compose exec broker \
          kafka-console-consumer --bootstrap-server broker:9092 --topic "$TOPIC" --from-beginning --property print.key=true --property key.separator=,
    ;;
    setup)
        curl -s -X POST -H "Content-Type: application/json" --data "{\"name\": \"lambda-sink\", \"config\": {\"connector.class\":\"io.github.zowens.kafkaconnectors.lambda.AwsLambdaSinkConnector\", \"tasks.max\":\"1\", \"topics\":\"$TOPIC\", \"connect.lambda.function.name\": \"test-func\"}}" http://localhost:8083/connectors
    ;;
    teardown)
        curl -s -X DELETE http://localhost:8083/connectors/lambda-sink
    ;;
    status)
        curl -s http://localhost:8083/connectors/lambda-sink/status
    ;;
    *)    # unknown option
        echo "Unknown option: $1"
        exit 1
    ;;
esac
