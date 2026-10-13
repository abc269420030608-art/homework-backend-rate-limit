package com.example.demo.controller;

import com.example.demo.model.LimitRuleRequest;
import com.example.demo.model.PageResponse;
import com.example.demo.model.RateLimitRule;
import com.example.demo.model.UsageResponse;
import com.example.demo.service.RateLimitService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping
public class RateLimitController {

    private final RateLimitService service;

    public RateLimitController(RateLimitService service) {
        this.service = service;
    }

    @PostMapping("/limits")
    public ResponseEntity<Void> createOrUpdate(@Valid @RequestBody LimitRuleRequest request) {
        service.createOrUpdate(
                new RateLimitRule(request.apiKey(), request.limit(), request.windowSeconds()));
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/check")
    public ResponseEntity<Void> check(@RequestParam @NotBlank String apiKey) {
        boolean allowed = service.check(apiKey).allowed();
        return ResponseEntity.status(allowed ? HttpStatus.OK : HttpStatus.TOO_MANY_REQUESTS).build();
    }

    @GetMapping("/usage")
    public UsageResponse usage(@RequestParam @NotBlank String apiKey) {
        return service.usage(apiKey);
    }

    @DeleteMapping("/limits/{apiKey}")
    public ResponseEntity<Void> delete(@PathVariable @NotBlank String apiKey) {
        service.delete(apiKey);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/limits")
    public PageResponse<RateLimitRule> list(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) int size) {
        return service.list(page, size);
    }
}
