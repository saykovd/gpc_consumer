package org.example;

import java.util.HashMap;
import java.util.Map;

public class MessageHeaders {

    private final Map<String, String> headers;

    public MessageHeaders() {
        headers = new HashMap<>();
    }

    public void addHeader(String key, String value) {
        headers.put(key, value);
    }
}