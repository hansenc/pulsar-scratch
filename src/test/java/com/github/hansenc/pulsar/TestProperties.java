package com.github.hansenc.pulsar;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestProperties {

    private final Properties properties;

    public TestProperties() {
        properties = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("test.properties")) {
            properties.load(stream);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getPulsarVersion() {
        return properties.getProperty("pulsar.version");
    }

    public String getJarHostPath() {
        return "target/" + properties.getProperty("project.build.finalName") + ".jar";
    }
}
