package com.github.hansenc.pulsar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * @see <a href="https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Testcontainer docs on singleton containers</a>
 */
@Testcontainers
public abstract class PulsarIntegrationTest {
    protected static final String FUNCTION_INPUT_TOPIC = "persistent://public/default/test-input";

    protected static final String FUNCTION_OUTPUT_TOPIC = "persistent://public/default/test-output";

    private static final String JAR_CONTAINER_PATH = "/app/lib/test-function.jar";

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarIntegrationTest.class);

    private static final TestProperties testProperties = new TestProperties();

    @Container
    private static final PulsarContainer PULSAR_CONTAINER;

    static {
        final Path jarHostPath = Paths.get(testProperties.getJarHostPath());
        checkState(Files.exists(jarHostPath) && Files.isRegularFile(jarHostPath), "Jar doesn't exist -- run 'mvn clean package' first");

        PULSAR_CONTAINER = new PulsarContainer(testProperties.getPulsarVersion())
                .withCommand("/pulsar/bin/pulsar", "standalone")
                .withCopyFileToContainer(forHostPath(jarHostPath), JAR_CONTAINER_PATH)
                .waitingFor(new LogMessageWaitStrategy()
                        .withRegEx(".*org.apache.pulsar.broker.PulsarService - messaging service is ready.*"))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("Pulsar Container")));
        LOGGER.info("Starting Pulsar container");
        PULSAR_CONTAINER.start();
    }

    protected static String getPulsarUrl() {
        return PULSAR_CONTAINER.getPulsarBrokerUrl();
    }

    protected static void createExclamationFunction() {
        LOGGER.info("Creating test function");
        try {
            final ExecResult result = PULSAR_CONTAINER.execInContainer("/pulsar/bin/pulsar-admin", "functions", "create",
                    "--jar", JAR_CONTAINER_PATH,
                    "--classname", ExclamationFunction.class.getName(),
                    "--tenant", "public",
                    "--namespace", "default",
                    "--inputs", FUNCTION_INPUT_TOPIC,
                    "--output", FUNCTION_OUTPUT_TOPIC
            );

            LOGGER.info("Create function result: {}", result);
            checkState(result.getExitCode() == 0, "Bad exit code: %s", result);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
