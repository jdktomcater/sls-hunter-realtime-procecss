package com.sls.hunter.realtime.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    // A poison record or failed send must not be silently skipped or committed.
    @Bean
    CommonErrorHandler errorHandler() {
        return new CommonContainerStoppingErrorHandler();
    }
}
