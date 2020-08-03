package com.github.hansenc.pulsar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static org.testcontainers.utility.MountableFile.forHostPath;

public abstract class TestUtils {
    static final String FUNCTION_INPUT_TOPIC = "persistent://public/default/test-input";

    static final String FUNCTION_OUTPUT_TOPIC = "persistent://public/default/test-output";

    private static final String JAR_CONTAINER_PATH = "/app/lib/test-function.jar";

    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

    private static final TestProperties testProperties = new TestProperties();

    static PulsarContainer createPulsarContainer() {
        final Path jarHostPath = Paths.get(testProperties.getJarHostPath());
        checkState(Files.exists(jarHostPath) && Files.isRegularFile(jarHostPath), "Jar doesn't exist -- run 'mvn clean package' first");

        return new PulsarContainer(testProperties.getPulsarVersion())
                .withCommand("/pulsar/bin/pulsar", "standalone")
                .withCopyFileToContainer(forHostPath(jarHostPath), JAR_CONTAINER_PATH)
                .waitingFor(new LogMessageWaitStrategy()
                        .withRegEx(".*org.apache.pulsar.broker.PulsarService - messaging service is ready.*"))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("Pulsar Container")));
    }

    static void createExclamationFunction(PulsarContainer container) {
        LOGGER.info("Creating test function");
        runInPulsar(container, "/pulsar/bin/pulsar-admin", "functions", "create",
                "--jar", JAR_CONTAINER_PATH,
                "--classname", ExclamationFunction.class.getName(),
                "--tenant", "public",
                "--namespace", "default",
                "--name", ExclamationFunction.class.getSimpleName(),
                "--inputs", FUNCTION_INPUT_TOPIC,
                "--output", FUNCTION_OUTPUT_TOPIC);
    }

    static void triggerExclamationFunction(PulsarContainer container, String triggerValue) {
        LOGGER.info("Triggering test function");
        runInPulsar(container, "/pulsar/bin/pulsar-admin", "functions", "trigger",
                "--name", ExclamationFunction.class.getSimpleName(),
                "--trigger-value", triggerValue);
    }

    private static void runInPulsar(PulsarContainer container, String... command) {
        final ExecResult result;
        try {
            result = container.execInContainer(command);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        LOGGER.info("Result: {}", result);
        checkState(result.getExitCode() == 0, "Bad exit code: %s", result);
    }
}
