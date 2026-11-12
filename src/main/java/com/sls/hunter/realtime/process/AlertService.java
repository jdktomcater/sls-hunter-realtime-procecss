package com.sls.hunter.realtime.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class AlertService {
    private static final Logger log = LoggerFactory.getLogger(AlertService.class);
    private final ObjectMapper mapper;
    private final AlertSettings settings;
    private final TelegramSender sender;

    AlertService(ObjectMapper mapper, AlertSettings settings, TelegramSender sender) {
        this.mapper = mapper;
        this.settings = settings;
        this.sender = sender;
    }

    @KafkaListener(topics = "${alert.topic}")
    public void consume(String payload, Acknowledgment ack) throws Exception {
        try {
            process(payload);
        } catch (Exception ex) {
            log.error("消息处理异常");
        }
        // Only commit the Kafka offset after the entire snapshot has been processed.
        ack.acknowledge();
    }

    void process(String payload) throws Exception {
        JsonNode root = mapper.readTree(payload);
        if (root == null || !root.isObject() || !"SNAPSHOT".equals(text(root, "kind")) || !"REPLACE".equals(text(root, "operation"))) {
            throw new IllegalArgumentException("Expected a REPLACE SNAPSHOT");
        }
        required(root, "index_family");
        boolean fixedWindow = root.has("window_start");
        if (fixedWindow) {
            required(root, "window_start");
        } else {
            required(root, "target_day");
        }
        JsonNode revisionNode = root.get("revision");
        JsonNode signals = root.get("all_signals");
        if (revisionNode == null || !revisionNode.isIntegralNumber() || !revisionNode.canConvertToLong() || revisionNode.longValue() < 1 || signals == null || !signals.isArray()) {
            return;
        }
        if (fixedWindow && settings.mode.equals("score") && !"none".equals(text(root, "baseline_mode"))) {
            return;
        }
        if (settings.mode.equals("channel") && (!fixedWindow || !"same_time_previous_days".equals(text(root, "baseline_mode")))) {
            return;
        }

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

        for (JsonNode signal : selected.values()) {
            sender.send(format(root, signal));
        }
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
        add(b, "目标日期", text(root, "target_day"));
        add(b, "窗口开始", text(root, "window_start"));
        add(b, "窗口结束", text(root, "window_end"));
        add(b, "状态", text(root, "status"));
        add(b, "版本", text(root, "revision"));
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
