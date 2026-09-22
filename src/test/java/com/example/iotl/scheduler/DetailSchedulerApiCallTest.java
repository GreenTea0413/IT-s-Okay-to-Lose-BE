package com.example.iotl.scheduler;

import com.example.iotl.global.response.BaseResponseServiceImpl;
import com.example.iotl.handler.ChartWebSocketHandler;
import com.example.iotl.handler.VolumeWebSocketHandler;
import com.example.iotl.service.stock.StockApiService;
import com.example.iotl.service.stock.StockService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 같은 종목을 보는 사용자 수가 늘어도 증권사 시세 API 호출이 늘지 않는지 센다.
 * 차트와 거래량 화면에 각각 USERS명이 삼성전자(005930) 하나를 구독한 상태에서 스케줄러를 한 번씩 돌린다.
 */
class DetailSchedulerApiCallTest {

    private static final int USERS = 10;
    private static final Map<String, String> OUTPUT = Map.of(
            "stck_oprc", "70000", "stck_hgpr", "71000", "stck_lwpr", "69000", "stck_prpr", "70500",
            "prdy_vrss", "500", "prdy_ctrt", "0.71", "acml_vol", "123456");

    @Test
    void 같은_종목_구독자가_늘어도_시세_API는_한_번만_부른다() throws Exception {
        RestTemplate restTemplate = mock(RestTemplate.class);
        when(restTemplate.postForEntity(anyString(), any(), eq(Map.class)))
                .thenReturn(ResponseEntity.ok(Map.of("access_token", "token")));
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), eq(Map.class)))
                .thenReturn(ResponseEntity.ok(Map.of("output", OUTPUT)));

        StockApiService api = new StockApiService(restTemplate);
        ReflectionTestUtils.setField(api, "baseUrl", "http://kis.test");

        BaseResponseServiceImpl baseResponse = new BaseResponseServiceImpl();
        StockService stockService = mock(StockService.class);
        ChartWebSocketHandler chartHandler = new ChartWebSocketHandler(baseResponse);
        VolumeWebSocketHandler volumeHandler = new VolumeWebSocketHandler(stockService, baseResponse);

        List<WebSocketSession> chartSessions = new ArrayList<>();
        List<WebSocketSession> volumeSessions = new ArrayList<>();
        for (int i = 0; i < USERS; i++) {
            WebSocketSession chart = session("chart-" + i);
            chartHandler.afterConnectionEstablished(chart);
            chartHandler.handleMessage(chart, new TextMessage("{\"codes\":[\"005930\"],\"interval\":\"live\"}"));
            chartSessions.add(chart);

            WebSocketSession volume = session("volume-" + i);
            volumeHandler.afterConnectionEstablished(volume);
            volumeHandler.handleMessage(volume, new TextMessage("005930"));
            volumeSessions.add(volume);
        }

        new ChartScheduler(api, chartHandler).sendChartDataToSubscribers();
        new VolumeScheduler(api, stockService, volumeHandler, baseResponse).sendVolumeData();

        // 사용자 전원이 값을 받았는지 먼저 확인한다. 호출을 줄이다 누락이 생기면 의미가 없다
        for (WebSocketSession s : chartSessions) verify(s, times(1)).sendMessage(any());
        for (WebSocketSession s : volumeSessions) verify(s, times(1)).sendMessage(any());

        int calls = mockingDetails(restTemplate).getInvocations().stream()
                .filter(inv -> inv.getMethod().getName().equals("exchange"))
                .toList().size();
        System.out.printf("차트 %d명 + 거래량 %d명, 종목 1개 → 시세 API 호출 %d회%n", USERS, USERS, calls);
        assertThat(calls).isEqualTo(1);
    }

    private static WebSocketSession session(String id) {
        WebSocketSession s = mock(WebSocketSession.class);
        when(s.getId()).thenReturn(id);
        when(s.isOpen()).thenReturn(true);
        return s;
    }
}
