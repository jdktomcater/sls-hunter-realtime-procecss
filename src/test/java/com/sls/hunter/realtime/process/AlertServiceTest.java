package com.sls.hunter.realtime.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    AlertSettings settings = new AlertSettings("score", "token", "chat", 70);
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
    AlertSettings settings = new AlertSettings("score", "token", "chat", 70);
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
    AlertSettings settings = new AlertSettings("score", "token", "chat", 70);
    FakeSender sender = new FakeSender(settings);
    AlertService service = new AlertService(new ObjectMapper(), settings, sender);
    assertThrows(IllegalArgumentException.class,
        () -> service.process(snapshot(1, signal("a", 80)).replace("\"none\"", "\"same_time_previous_days\"")));
    assertEquals(0, sender.calls);
  }

  static class FakeSender extends TelegramSender {
    int calls;
    boolean failSecond;

    FakeSender(AlertSettings settings) {
      super(new ObjectMapper(), settings);
    }

    @Override
    void send(String text) {
      calls++;
      if (failSecond && calls == 2) {
        throw new IllegalStateException("failure");
      }
    }
  }
}
