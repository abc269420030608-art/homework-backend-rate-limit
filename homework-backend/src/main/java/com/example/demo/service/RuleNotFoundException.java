package com.example.demo.service;

public class RuleNotFoundException extends RuntimeException {
    public RuleNotFoundException(String apiKey) {
        super("No rate-limit rule exists for apiKey: " + apiKey);
    }
}
