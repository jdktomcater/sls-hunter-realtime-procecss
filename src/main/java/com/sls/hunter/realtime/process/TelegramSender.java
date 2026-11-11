package com.sls.hunter.realtime.process;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class TelegramSender {
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final TelegramSettings settings;

    TelegramSender(ObjectMapper mapper, TelegramSettings settings) {
        this.mapper = mapper;
        this.settings = settings;
        this.client = HttpClient.newBuilder().connectTimeout(settings.connectTimeout).build();
    }

    void send(String message) throws Exception {
        String body = mapper.writeValueAsString(Map.of("chat_id", settings.chatId, "text", message));
        HttpRequest request = HttpRequest.newBuilder(settings.sendMessageUri()).timeout(settings.requestTimeout).header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Telegram request interrupted");
        } catch (Exception e) {
            throw new IllegalStateException("Telegram request failed");
        }
        if (response.statusCode() != 200 || !mapper.readTree(response.body()).path("ok").asBoolean(false)) {
            throw new IllegalStateException("Telegram request rejected, HTTP " + response.statusCode());
        }
    }
}
