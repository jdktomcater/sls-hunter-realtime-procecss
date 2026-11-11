package com.sls.hunter.realtime.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

@SpringBootTest(properties = {
    "alert.bot-token=old-token",
    "alert.chat-id=old-chat",
    "telegram.api-base-url=https://example.org/telegram/",
    "telegram.connect-timeout=2s",
    "telegram.request-timeout=3s",
    "spring.kafka.listener.auto-startup=false"
})
@TestExecutionListeners(DependencyInjectionTestExecutionListener.class)
class TelegramSettingsTest {
    @Autowired TelegramSettings settings;

    @Test
    void bindsConfigurationAndSupportsLegacyCredentials() {
        assertEquals("old-chat", settings.chatId);
        assertEquals(URI.create("https://example.org/telegram/botold-token/sendMessage"), settings.sendMessageUri());
        assertEquals(Duration.ofSeconds(2), settings.connectTimeout);
        assertEquals(Duration.ofSeconds(3), settings.requestTimeout);
    }

    @Test
    void rejectsInvalidTimeoutAndBaseUrl() {
        assertThrows(IllegalArgumentException.class, () -> new TelegramSettings("token", "chat",
            URI.create("https://example.org"), Duration.ZERO, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new TelegramSettings("token", "chat",
            URI.create("https://example.org/?query=1"), Duration.ofSeconds(1), Duration.ofSeconds(1)));
    }
}
