package com.sls.hunter.realtime.process;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AlertSettings {
  final String mode, stateFile, token, chatId;
  final double minScore;
  AlertSettings(@Value("${alert.risk-mode}") String mode, @Value("${alert.state-file}") String stateFile,
                @Value("${alert.bot-token}") String token, @Value("${alert.chat-id}") String chatId,
                @Value("${alert.min-score}") double minScore) {
    if (!mode.equals("score") && !mode.equals("channel")) throw new IllegalArgumentException("RISK_MODE must be score or channel");
    if (token.isBlank() || chatId.isBlank()) throw new IllegalArgumentException("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required");
    if (!Double.isFinite(minScore)) throw new IllegalArgumentException("MIN_SCORE must be finite");
    this.mode=mode; this.stateFile=stateFile; this.token=token; this.chatId=chatId; this.minScore=minScore;
  }
}
