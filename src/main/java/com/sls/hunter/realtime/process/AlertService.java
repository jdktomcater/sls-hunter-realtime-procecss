package com.sls.hunter.realtime.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class AlertService {
    private final ObjectMapper mapper;
    private final AlertSettings settings;
    private final AlertStore store;
    private final TelegramSender sender;

    AlertService(ObjectMapper mapper, AlertSettings settings, AlertStore store, TelegramSender sender) {
        this.mapper = mapper;
        this.settings = settings;
        this.store = store;
        this.sender = sender;
    }

    @KafkaListener(topics = "${alert.topic}")
    public void consume(String payload, Acknowledgment ack) throws Exception {
        process(payload);
        // Only commit the Kafka offset after the entire snapshot has been processed.
        ack.acknowledge();
    }

    void process(String payload) throws Exception {
        JsonNode root = mapper.readTree(payload);
        if (root == null || !root.isObject() || !"SNAPSHOT".equals(text(root, "kind")) || !"REPLACE".equals(text(root, "operation"))) {
            throw new IllegalArgumentException("Expected a REPLACE SNAPSHOT");
        }
        String family = required(root, "index_family");
        String window = required(root, "window_start");
        JsonNode revisionNode = root.get("revision");
        JsonNode signals = root.get("all_signals");
        if (revisionNode == null || !revisionNode.isIntegralNumber() || !revisionNode.canConvertToLong() || revisionNode.longValue() < 1 || signals == null || !signals.isArray()) {
            throw new IllegalArgumentException("Invalid revision or all_signals");
        }
        if (settings.mode.equals("score") && !"none".equals(text(root, "baseline_mode"))) {
            throw new IllegalArgumentException("Score mode requires baseline_mode=none");
        }
        if (settings.mode.equals("channel") && !"same_time_previous_days".equals(text(root, "baseline_mode"))) {
            throw new IllegalArgumentException("Channel mode requires baseline_mode=same_time_previous_days");
        }

        long revision = revisionNode.longValue();
        Map<String, JsonNode> selected = new LinkedHashMap<>();
        Set<String> ids = new HashSet<>();
        for (JsonNode signal : signals) {
            String id = required(signal, "signal_id");
            if (!ids.add(id)) {
                throw new IllegalArgumentException("Duplicate signal ID");
            }
            if (settings.mode.equals("channel") && !"high_risk".equals(text(signal, "channel"))) {
                continue;
            }
            JsonNode score = signal.get("score");
            if (score == null || !score.isNumber() || !Double.isFinite(score.doubleValue())) {
                throw new IllegalArgumentException("Invalid signal score");
            }
            if (score.doubleValue() >= settings.minScore) {
                selected.put(id, signal);
            }
        }

        if (revision <= store.revision(family, window)) {
            return;
        }
        Set<String> active = store.active(family, window);
        for (var entry : selected.entrySet()) {
            String id = entry.getKey();
            if (!active.contains(id) && !store.delivered(family, window, revision, id)) {
                sender.send(format(root, entry.getValue()));
                // Persist each successful send so a later failure can be replayed without resending it.
                store.markDelivered(family, window, revision, id);
            }
        }
        store.finish(family, window, revision, selected.keySet());
    }

    static String text(JsonNode n, String key) {
        JsonNode value = n.get(key);
        return value != null && value.isValueNode() && !value.isNull() ? value.asText() : "";
    }

    static String required(JsonNode n, String key) {
        if (n == null || !n.isObject() || !n.path(key).isTextual() || n.path(key).asText().isBlank()) {
            throw new IllegalArgumentException("Missing " + key);
        }
        return n.path(key).asText();
    }

    static String format(JsonNode root, JsonNode signal) {
        StringBuilder b = new StringBuilder("🚨 高危安全告警");
        String description = text(signal, "description");
        if (description.isBlank()) {
            description = text(signal, "behavior_label");
        }
        if (description.isBlank()) {
            description = text(signal, "signal_type");
        }
        add(b, "告警", description);
        add(b, "风险等级", text(signal, "severity"));
        add(b, "评分", text(signal, "score"));
        add(b, "资产", text(signal, "host_ip"));
        add(b, "用户", text(signal, "username"));
        add(b, "进程", text(signal, "proc_name"));
        add(b, "命中次数", text(signal, "hit_count"));
        add(b, "数据族", text(root, "index_family"));
        add(b, "窗口开始", text(root, "window_start"));
        add(b, "窗口结束", text(root, "window_end"));
        add(b, "信号 ID", text(signal, "signal_id"));
        return b.toString();
    }

    private static void add(StringBuilder b, String label, String value) {
        if (!value.isBlank()) {
            value = value.replace('\n', ' ').replace('\r', ' ');
            b.append('\n').append(label).append('：').append(value, 0, Math.min(value.length(), 250));
        }
    }
}
