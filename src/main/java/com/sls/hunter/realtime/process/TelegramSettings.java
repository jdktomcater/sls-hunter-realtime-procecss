package com.sls.hunter.realtime.process;

import java.net.URI;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TelegramSettings {
    final String token;
    final String chatId;
    final URI apiBaseUrl;
    final Duration connectTimeout;
    final Duration requestTimeout;

    TelegramSettings(@Value("${telegram.bot-token}") String token,
                     @Value("${telegram.chat-id}") String chatId,
                     @Value("${telegram.api-base-url}") URI apiBaseUrl,
                     @Value("${telegram.connect-timeout}") Duration connectTimeout,
                     @Value("${telegram.request-timeout}") Duration requestTimeout) {
        if (token.isBlank() || chatId.isBlank()) {
            throw new IllegalArgumentException("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required");
        }
        if ((!"https".equalsIgnoreCase(apiBaseUrl.getScheme()) && !"http".equalsIgnoreCase(apiBaseUrl.getScheme()))
                || apiBaseUrl.getHost() == null || apiBaseUrl.getUserInfo() != null
                || apiBaseUrl.getQuery() != null || apiBaseUrl.getFragment() != null) {
            throw new IllegalArgumentException("TELEGRAM_API_BASE_URL must be an HTTP(S) URL without credentials, query or fragment");
        }
        if (connectTimeout.isZero() || connectTimeout.isNegative() || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("Telegram timeouts must be positive");
        }
        this.token = token;
        this.chatId = chatId;
        this.apiBaseUrl = apiBaseUrl;
        this.connectTimeout = connectTimeout;
        this.requestTimeout = requestTimeout;
    }

    URI sendMessageUri() {
        String base = apiBaseUrl.toString();
        return URI.create((base.endsWith("/") ? base : base + "/") + "bot" + token + "/sendMessage");
    }
}
