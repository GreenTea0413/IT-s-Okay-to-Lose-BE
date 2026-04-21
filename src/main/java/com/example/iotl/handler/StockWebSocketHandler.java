package com.example.iotl.handler;

import com.example.iotl.global.response.BaseResponse;
import com.example.iotl.global.response.BaseResponseService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockWebSocketHandler extends TextWebSocketHandler {

    private final List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BaseResponseService baseResponseService;

    private boolean marketOpen = true;

    public void setMarketOpen(boolean open) {
        this.marketOpen = open;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
    }

    public void broadcast(Object rawData) {
        if (!marketOpen) return;

        BaseResponse<Object> response = baseResponseService.getSuccessResponse(rawData);
        TextMessage message;
        try {
            message = new TextMessage(objectMapper.writeValueAsString(response));
        } catch (IOException e) {
            log.error("❌ WebSocket 메시지 직렬화 실패", e);
            return;
        }

        for (WebSocketSession session : sessions) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(message);
                }
            } catch (IOException e) {
                log.error("❌ WebSocket 메시지 전송 중 오류 발생: {}", e.getMessage());
                sessions.remove(session);
            }
        }
    }

    public void closeAllSessions() {
        for (WebSocketSession session : sessions) {
            try {
                if (session.isOpen()) session.close();
            } catch (IOException e) {
                log.error("❌ 세션 닫기 실패", e);
            }
        }
        sessions.clear();
    }
}