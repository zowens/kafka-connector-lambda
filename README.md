# Kafka Connector - AWS Lambda

[Kafka Connect](https://docs.confluent.io/current/connect/index.html) framework plugin for invoking AWS Lambda with a batch of records from Kafka.

## Quickstart

### Prerequisites 
* docker
* docker-compose
* Java 8
* AWS CLI (configured with credentials at ~/.aws/credentials)
* [IAM Role for Lambda Exectution](https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-create-iam-role.html)


### Build/Run

```bash
# Build the library with Gradle
./gradlew build

# Deploy to a local docker environment
docker-compose up -d
```

### Create Lambda

```bash
zip -j sample_lambda.zip example/lambda.js
aws lambda create-function --function-name test-func --runtime nodejs6.10 --zip-file fileb://sample_lambda.zip --handler lambda.handler --role arn:aws:iam::ACCOUNT_NUM:role/lambda_role
```

### Setup the connector

```bash
./example/run.sh setup
```

### Produce Messages

```bash
./example/run.sh produce
```

In the logs of the Lambda, you should see the payload logged with the messages produced from Kafka:

```javascript
{
    "Records": [
        {
            "Key": "foo",
            "Value": "baz",
            "Offset": 0
        }
    ]
}
```
