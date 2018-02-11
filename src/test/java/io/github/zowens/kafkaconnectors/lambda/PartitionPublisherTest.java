package io.github.zowens.kafkaconnectors.lambda;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.databind.JsonNode;

import net.jodah.failsafe.RetryPolicy;

@RunWith(MockitoJUnitRunner.class)
public class PartitionPublisherTest {
    private static final String FUNCTION_NAME = "testfunc";
    private static final String TOPIC = "testtopic";
    private static final int PARTITION = 10;

    private AWSLambdaAsync lambdaClient;
    private PartitionPublisher publisher;
    private int offsetTracker = 0;

    @Captor
    public ArgumentCaptor<InvokeRequest> requestCaptor;

    @Captor
    public ArgumentCaptor<AsyncHandler<InvokeRequest, InvokeResult>> handlerCaptor;

    @Before
    public void start() {
        this.lambdaClient = Mockito.mock(AWSLambdaAsync.class);
        this.publisher = new PartitionPublisher(
            new TopicPartition(TOPIC, PARTITION),
            FUNCTION_NAME,
            lambdaClient,
            new RetryPolicy()
        );
        offsetTracker = 0;
    }

    @After
    public void stop() {
        this.publisher.stop();
    }

    @Test
    public void testQueueDoesNotSend() {
        this.publisher.queue(createRecord("1", "test1"));
        this.publisher.queue(createRecord("2", "test2"));
        this.publisher.queue(createRecord("3", "test3"));
        this.publisher.queue(createRecord("4", "test4"));

        Mockito.verifyZeroInteractions(this.lambdaClient);
    }

    @Test
    public void testTrySendIgnoresEmptyQueue() {
        this.publisher.trySend(false);
        Mockito.verifyZeroInteractions(this.lambdaClient);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTrySendInvokesLambda() throws Exception {
        final CountDownLatch invokeSignal = new CountDownLatch(1);
        Mockito.when(lambdaClient.invokeAsync(Mockito.any(), Mockito.any()))
                .then((InvocationOnMock i) -> {
                    InvokeRequest request = (InvokeRequest)i.getArguments()[0];
                    ((AsyncHandler<InvokeRequest, InvokeResult>)i.getArguments()[1])
                        .onSuccess(request, new InvokeResult()
                                .withStatusCode(200));
                    invokeSignal.countDown();
                    return (Future<InvokeResult>)null;
                });

        this.publisher.queue(createRecord("1", "test1"));
        this.publisher.queue(createRecord("2", "test2"));
        this.publisher.queue(createRecord("3", "test3"));

        this.publisher.trySend(false);

        invokeSignal.await();

        Mockito.verify(lambdaClient).invokeAsync(
                requestCaptor.capture(),
                handlerCaptor.capture());

        Assert.assertEquals(FUNCTION_NAME, requestCaptor.getValue().getFunctionName());

        JsonNode tree = Constants.OBJECT_MAPPER.readTree(
                Base64.getDecoder().decode(requestCaptor.getValue().getClientContext()));
        Assert.assertTrue(tree.isObject());
        Assert.assertEquals(PARTITION, tree.get("partition").asInt());
        Assert.assertEquals(TOPIC, tree.get("topic").asText());

        JsonNode payload = Constants.OBJECT_MAPPER.readTree(requestCaptor.getValue().getPayload().array());
        Assert.assertTrue(payload.isObject());
        Assert.assertTrue(payload.has("Records"));

        List<JsonNode> records = new ArrayList<>();
        payload.get("Records").forEach(records::add);
        Assert.assertEquals(3, records.size());

        Assert.assertEquals(records.get(0), Constants.OBJECT_MAPPER.createObjectNode()
                .put("Key", "1")
                .put("Value", "test1")
                .put("Offset", 0));

        Assert.assertEquals(records.get(1), Constants.OBJECT_MAPPER.createObjectNode()
                .put("Key", "2")
                .put("Value", "test2")
                .put("Offset", 1));

        Assert.assertEquals(records.get(2), Constants.OBJECT_MAPPER.createObjectNode()
                .put("Key", "3")
                .put("Value", "test3")
                .put("Offset", 2));
    }

    @Test
    public void testTrySendDoesNotInvokeWithOutstandingInvocation() {
        this.publisher.queue(createRecord("1", "test1"));
        this.publisher.queue(createRecord("2", "test2"));
        this.publisher.queue(createRecord("3", "test3"));
        this.publisher.trySend(false);

        this.publisher.queue(createRecord("4", "test4"));
        this.publisher.trySend(false);

        Mockito.verify(lambdaClient, Mockito.times(1)).invokeAsync(
                requestCaptor.capture(),
                handlerCaptor.capture());

        Assert.assertEquals(1, this.publisher.queuedRecords());
    }

    @Test
    public void testTrySendOnSuccessInvokesTrySendAgain() throws Exception {
        this.publisher.queue(createRecord("1", "test1"));
        this.publisher.trySend(true);
        this.publisher.queue(createRecord("2", "test2"));
        this.publisher.trySend(true);

        Mockito.verify(lambdaClient, Mockito.times(1)).invokeAsync(
                requestCaptor.capture(), handlerCaptor.capture());

        Assert.assertEquals(1, this.publisher.queuedRecords());

        // invoke the handler
        handlerCaptor.getValue().onSuccess(
                requestCaptor.getValue(),
                new InvokeResult()
                    .withPayload(ByteBuffer.allocate(0))
                    .withLogResult("")
                    .withStatusCode(200)
        );

        Mockito.verify(lambdaClient, Mockito.times(2)).invokeAsync(
                Mockito.any(), Mockito.any());
    }

    @Test
    public void testTestLambdaLimit() {
        // form 2 messages, where the length of each is half of the lambda limit. should only send 1
        char[] msg1 = new char[Constants.LAMBDA_MAX_BYTES / 2];
        Arrays.fill(msg1, 'A');
        char[] msg2 = new char[Constants.LAMBDA_MAX_BYTES / 2];
        Arrays.fill(msg2, 'B');

        this.publisher.queue(createRecord("1", new String(msg1)));
        this.publisher.queue(createRecord("2", new String(msg2)));

        this.publisher.trySend(true);

        Assert.assertEquals(1, this.publisher.queuedRecords());
    }

    @Test
    public void testTrySendLambdaInvokeRetries() throws Exception {
        final CountDownLatch invokeSignal = new CountDownLatch(2);
        Mockito.when(lambdaClient.invokeAsync(Mockito.any(), Mockito.any()))
                // fail first
                .thenAnswer(invokeMock(invokeSignal, "Handled"))
                // then succeed
                .thenAnswer(invokeMock(invokeSignal, null));

        this.publisher.queue(createRecord("1", "test1"));
        this.publisher.trySend(true);

        invokeSignal.await();

        // TODO: fix flakiness with await running before return
        // rather than adding a sleep :(
        Thread.sleep(50);

        Mockito.verify(lambdaClient, Mockito.times(2)).invokeAsync(
                requestCaptor.capture(), handlerCaptor.capture());

        Assert.assertArrayEquals(
            requestCaptor.getAllValues().get(0).getPayload().array(),
            requestCaptor.getAllValues().get(1).getPayload().array());
    }

    @Test
    public void testFlushDrainsQueue() throws Exception {
        final CountDownLatch invokeSignal = new CountDownLatch(2);
        Mockito.when(lambdaClient.invokeAsync(Mockito.any(), Mockito.any()))
            .then(invokeMock(invokeSignal, null));

        this.publisher.queue(createRecord("1", "test1"));
        this.publisher.queue(createRecord("2", "test2"));
        this.publisher.trySend(false);

        this.publisher.queue(createRecord("3", "test3"));
        this.publisher.flush();
        invokeSignal.await();

        Mockito.verify(lambdaClient, Mockito.times(2)).invokeAsync(
                requestCaptor.capture(), handlerCaptor.capture());
    }

    @SuppressWarnings("unchecked")
    private Answer<Future<InvokeResult>> invokeMock(CountDownLatch latch, String error) {
        return (InvocationOnMock i) -> {
            InvokeRequest request = (InvokeRequest)i.getArguments()[0];
            ((AsyncHandler<InvokeRequest, InvokeResult>)i.getArguments()[1])
                .onSuccess(request, new InvokeResult()
                        .withStatusCode(200)
                        .withFunctionError(error));
            latch.countDown();
            return (Future<InvokeResult>)null;
        };
    }

    private SinkRecord createRecord(String key, String value) {
        return new SinkRecord(
            TOPIC,
            PARTITION,
            Schema.STRING_SCHEMA,
            key,
            Schema.STRING_SCHEMA,
            value,
            offsetTracker++
        );
    }
}
