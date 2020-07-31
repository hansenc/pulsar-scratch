package com.github.hansenc.pulsar;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class ExclamationFunction implements Function<String, String> {

    public ExclamationFunction() {
        System.out.println("new ExclamationFunction()");
    }

    @Override
    public String process(String input, Context context) {
        context.getLogger().info("Input: {}", input);
        return input + "!";
    }
}
