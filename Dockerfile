FROM confluentinc/cp-kafka-connect-base
RUN mkdir -p /usr/share/java/kafka-connect-lambda
COPY build/libs/kafka-connector-lambda-*-all.jar /usr/share/java/kafka-connect-lambda
