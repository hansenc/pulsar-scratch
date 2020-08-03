package com.github.hansenc.pulsar;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.github.hansenc.pulsar.TestUtils.FUNCTION_INPUT_TOPIC;
import static com.github.hansenc.pulsar.TestUtils.FUNCTION_OUTPUT_TOPIC;
import static com.github.hansenc.pulsar.TestUtils.createExclamationFunction;
import static com.github.hansenc.pulsar.TestUtils.createPulsarContainer;
import static com.github.hansenc.pulsar.TestUtils.triggerExclamationFunction;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class FunctionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionIT.class);

    @Container
    public PulsarContainer pulsarContainer = createPulsarContainer();

    @Test
    public void testWithTrigger() throws PulsarClientException, InterruptedException {
        createExclamationFunction(pulsarContainer);
        Thread.sleep(2000); //briefly wait for function to be ready
        triggerExclamationFunction(pulsarContainer, "zero");
        runTest(Arrays.asList("one", "two", "three"),
                Arrays.asList("one!", "two!", "three!")); //note: the trigger value does not show up in the output topic, but I'm not sure if that should be expected or not
    }

    /**
     * This test hangs unexpectedly and it doesn't seem that the function is ever called.
     */
    @Test
    public void testWithoutTrigger() throws PulsarClientException, InterruptedException {
        createExclamationFunction(pulsarContainer);
        Thread.sleep(2000); //briefly wait for function to be ready
        runTest(Arrays.asList("one", "two", "three"), Arrays.asList("one!", "two!", "three!"));
    }

    private void runTest(Collection<String> produceValues, List<String> expectedOutput) throws PulsarClientException {
        LOGGER.info("Creating client");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarContainer.getPulsarBrokerUrl())
                .build()) {
            LOGGER.info("Producing to {}", FUNCTION_INPUT_TOPIC);
            try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(FUNCTION_INPUT_TOPIC)
                    .producerName("test-producer")
                    .compressionType(CompressionType.LZ4)
                    .messageRoutingMode(MessageRoutingMode.SinglePartition)
                    .create()) {
                LOGGER.info("Consuming from {}", FUNCTION_OUTPUT_TOPIC);
                try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                        .topic(FUNCTION_OUTPUT_TOPIC)
                        .consumerName("test-consumer")
                        .subscriptionName("test-sub")
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscribe()) {

                    LOGGER.info("Begin producing");
                    for (String s : produceValues) {
                        LOGGER.info("Producing: {}", s);
                        producer.send(s);
                    }
                    producer.flush();
                    LOGGER.info("Done producing");

                    LOGGER.info("Begin consuming");
                    int received = 0;
                    while (received < expectedOutput.size()) {
                        final Message<String> message = consumer.receive();
                        LOGGER.info("Received: {}", message.getValue());
                        consumer.acknowledge(message);
                        assertThat(message.getValue()).isEqualTo(expectedOutput.get(received++));
                    }
                    LOGGER.info("Done consuming");
                }
            }
        }
    }
}
