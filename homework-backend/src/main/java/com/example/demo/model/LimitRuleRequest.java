package com.example.demo.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record LimitRuleRequest(
        @NotBlank @Size(max = 128) String apiKey,
        @Positive int limit,
        @Positive @Max(86_400) long windowSeconds) {
}
