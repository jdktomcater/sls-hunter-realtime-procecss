package com.sls.hunter.realtime.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class AlertServiceTest {
  private String snapshot(int revision, String signals) {
    return """
        {"kind":"SNAPSHOT","operation":"REPLACE","index_family":"linux","window_start":"2026-01-01T00:00:00Z","baseline_mode":"none","revision":%d,"all_signals":[%s]}
        """.formatted(revision, signals);
  }

  private String signal(String id, int score) {
    return "{\"signal_id\":\"" + id + "\",\"score\":" + score + "}";
  }

  @Test
  void sendsMatchingSignalsFromEverySnapshotAndAcknowledgesAfterSending() throws Exception {
    AlertSettings settings = new AlertSettings("score", 70);
    FakeSender sender = new FakeSender(settings);
    AlertService service = new AlertService(new ObjectMapper(), settings, sender);
    int[] count = {0};
    Acknowledgment ack = () -> count[0]++;
    String record = snapshot(1, signal("a", 70) + "," + signal("b", 69));
    service.consume(record, ack);
    service.consume(record, ack);
    assertEquals(2, sender.calls);
    assertEquals(2, count[0]);
  }

  @Test
  void failedSendDoesNotAcknowledgeAndReplayCanRepeatPriorAlerts() throws Exception {
    AlertSettings settings = new AlertSettings("score", 70);
    FakeSender sender = new FakeSender(settings);
    AlertService service = new AlertService(new ObjectMapper(), settings, sender);
    int[] count = {0};
    Acknowledgment ack = () -> count[0]++;
    sender.failSecond = true;
    String record = snapshot(1, signal("a", 70) + "," + signal("b", 80));
    assertThrows(IllegalStateException.class, () -> service.consume(record, ack));
    assertEquals(0, count[0]);
    service.consume(record, ack);
    assertEquals(4, sender.calls);
    assertEquals(1, count[0]);
  }

  @Test
  void rejectsIncompatibleModeBeforeSending() throws Exception {
    AlertSettings settings = new AlertSettings("score", 70);
    FakeSender sender = new FakeSender(settings);
    AlertService service = new AlertService(new ObjectMapper(), settings, sender);
    assertThrows(IllegalArgumentException.class,
        () -> service.process(snapshot(1, signal("a", 80)).replace("\"none\"", "\"same_time_previous_days\"")));
    assertEquals(0, sender.calls);
  }

  @Test
  void processesDailySnapshotsAndIncludesTheirDateAndRevision() throws Exception {
    AlertSettings settings = new AlertSettings("score", 70);
    FakeSender sender = new FakeSender(settings);
    AlertService service = new AlertService(new ObjectMapper(), settings, sender);
    String payload = """
        {"kind":"SNAPSHOT","operation":"REPLACE","index_family":"tracing_sas_linux",
         "target_day":"2026-01-01","revision":2,"status":"CORRECTED",
         "all_signals":[{"signal_id":"a","score":80,"description":"test"}]}
        """;
    service.process(payload);
    assertEquals(1, sender.calls);
    assertTrue(sender.lastMessage.contains("目标日期：2026-01-01"));
    assertTrue(sender.lastMessage.contains("版本：2"));
  }

  @Test
  void channelModeSelectsOnlyHighRiskAndRejectsDailySnapshots() throws Exception {
    AlertSettings settings = new AlertSettings("channel", 70);
    FakeSender sender = new FakeSender(settings);
    AlertService service = new AlertService(new ObjectMapper(), settings, sender);
    String signals = "{\"signal_id\":\"a\",\"channel\":\"high_risk\",\"score\":70},"
        + "{\"signal_id\":\"b\",\"channel\":\"history\",\"score\":100}";
    service.process(snapshot(1, signals).replace("\"none\"", "\"same_time_previous_days\""));
    assertEquals(1, sender.calls);
    assertThrows(IllegalArgumentException.class, () -> service.process(
        "{\"kind\":\"SNAPSHOT\",\"operation\":\"REPLACE\",\"index_family\":\"linux\","
            + "\"target_day\":\"2026-01-01\",\"revision\":1,\"all_signals\":[]}"));
  }

  @Test
  void validatesEntireSnapshotBeforeSending() {
    AlertSettings settings = new AlertSettings("score", 70);
    FakeSender sender = new FakeSender(settings);
    AlertService service = new AlertService(new ObjectMapper(), settings, sender);
    assertThrows(IllegalArgumentException.class,
        () -> service.process(snapshot(1, signal("a", 80) + "," + signal("a", 90))));
    assertEquals(0, sender.calls);
  }

  static class FakeSender extends TelegramSender {
    int calls;
    boolean failSecond;
    String lastMessage;

    FakeSender(AlertSettings settings) {
      super(new ObjectMapper(), new TelegramSettings("token", "chat", java.net.URI.create("https://api.telegram.org"), java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(20)));
    }

    @Override
    void send(String text) {
      calls++;
      lastMessage = text;
      if (failSecond && calls == 2) {
        throw new IllegalStateException("failure");
      }
    }
  }
}
