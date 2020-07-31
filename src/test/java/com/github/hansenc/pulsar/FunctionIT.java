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

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FunctionIT extends PulsarIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionIT.class);

    @Test
    public void basicFunctionTest() throws PulsarClientException {
        createExclamationFunction();
        LOGGER.info("Creating client");
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(getPulsarUrl())
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
                    final List<String> input = Arrays.asList("one", "two", "three");

                    final List<String> expected = Arrays.asList("one!", "two!", "three!");

                    LOGGER.info("Begin producing");
                    for (String s : input) {
                        LOGGER.info("Producing: {}", s);
                        producer.send(s);
                    }
                    producer.flush();
                    LOGGER.info("Done producing");

                    LOGGER.info("Begin consuming");
                    int received = 0;
                    while (received < expected.size()) {
                        final Message<String> message = consumer.receive();
                        LOGGER.info("Received: {}", message.getValue());
                        consumer.acknowledge(message);
                        assertThat(message.getValue()).isEqualTo(expected.get(received++));
                    }
                    LOGGER.info("Done consuming");
                }
            }
        }
    }
}
