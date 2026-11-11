package com.sls.hunter.realtime.process;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AlertSettings {
    final String mode;
    final double minScore;

    AlertSettings(@Value("${alert.risk-mode}") String mode,
                  @Value("${alert.min-score}") double minScore) {
        if (!mode.equals("score") && !mode.equals("channel")) {
            throw new IllegalArgumentException("RISK_MODE must be score or channel");
        }
        if (!Double.isFinite(minScore)) {
            throw new IllegalArgumentException("MIN_SCORE must be finite");
        }
        this.mode = mode;
        this.minScore = minScore;
    }
}
