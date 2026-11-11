package com.sls.hunter.realtime.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.support.Acknowledgment;

class AlertServiceTest {
  @TempDir Path dir;

  private String snapshot(int revision, String signals) {
    return """
        {"kind":"SNAPSHOT","operation":"REPLACE","index_family":"linux","window_start":"2026-01-01T00:00:00Z","baseline_mode":"none","revision":%d,"all_signals":[%s]}
        """.formatted(revision, signals);
  }

  private String signal(String id, int score) {
    return "{\"signal_id\":\"" + id + "\",\"score\":" + score + "}";
  }

  @Test
  void revisionsAndFailuresKeepSuccessfulDeliveries() throws Exception {
    AlertSettings settings = new AlertSettings("score", dir.resolve("state.db").toString(), "token", "chat", 70);
    FakeSender sender = new FakeSender(settings);
    try (AlertStoreWrapper wrapper = new AlertStoreWrapper(new AlertStore(settings))) {
      AlertService service = new AlertService(new ObjectMapper(), settings, wrapper.store, sender);
      int[] count = {0};
      Acknowledgment ack = () -> count[0]++;
      sender.failSecond = true;
      String record = snapshot(1, signal("a", 70) + "," + signal("b", 80));
      assertThrows(IllegalStateException.class, () -> service.consume(record, ack));
      assertEquals(0, count[0]);
      service.consume(record, ack);
      assertEquals(3, sender.calls);
      assertEquals(1, count[0]);
      service.process(snapshot(2, signal("a", 70) + "," + signal("b", 80)));
      service.process(snapshot(1, signal("c", 90)));
      assertEquals(3, sender.calls);
      service.process(snapshot(3, signal("b", 80)));
      service.process(snapshot(4, signal("a", 70) + "," + signal("b", 80)));
      assertEquals(4, sender.calls);
    }
  }

  @Test
  void rejectsIncompatibleModeBeforeSending() throws Exception {
    AlertSettings settings = new AlertSettings("score", dir.resolve("state.db").toString(), "token", "chat", 70);
    FakeSender sender = new FakeSender(settings);
    try (AlertStoreWrapper wrapper = new AlertStoreWrapper(new AlertStore(settings))) {
      AlertService service = new AlertService(new ObjectMapper(), settings, wrapper.store, sender);
      assertThrows(IllegalArgumentException.class,
          () -> service.process(snapshot(1, signal("a", 80)).replace("\"none\"", "\"same_time_previous_days\"")));
      assertEquals(0, sender.calls);
    }
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

  static class AlertStoreWrapper implements AutoCloseable {
    final AlertStore store;

    AlertStoreWrapper(AlertStore store) {
      this.store = store;
    }

    public void close() throws Exception {
      store.close();
    }
  }
}
