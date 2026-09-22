package com.example.iotl.service.stock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Component
@RequiredArgsConstructor
public class StockApiService {
    private final RestTemplate restTemplate;

    @Value("${kis.api.base-url}")
    private String baseUrl;

    @Value("${kis.api.appkey}")
    private String appKey;

    @Value("${kis.api.appsecret}")
    private String appSecret;

    private String accessToken;

    public String getAccessToken() {
        if (accessToken != null) return accessToken;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, String> body = new HashMap<>();
        body.put("grant_type", "client_credentials");
        body.put("appkey", appKey);
        body.put("appsecret", appSecret);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
        ResponseEntity<Map> response = restTemplate.postForEntity(baseUrl + "/oauth2/tokenP", request, Map.class);
        accessToken = (String) response.getBody().get("access_token");

        return accessToken;
    }

    // 같은 종목을 여러 화면과 사용자가 동시에 요청해도 증권사 API는 종목당 한 번만 부르도록 잠깐 보관한다
    // ponytail: 서버 한 대 기준 메모리 보관. 서버를 여러 대로 늘리면 Redis 같은 공용 저장소로 옮긴다
    private static final long PRICE_TTL_MS = 3000;
    private record CachedPrice(long fetchedAt, Map<String, Object> body) {}
    private final Map<String, CachedPrice> priceCache = new ConcurrentHashMap<>();

    public Map<String, Object> getStockPrice(String code) {
        // compute는 같은 종목에 대한 동시 요청을 줄 세우므로 두 스케줄러가 같은 순간에 불러도 호출은 한 번이다
        return priceCache.compute(code, (k, cached) ->
                cached != null && System.currentTimeMillis() - cached.fetchedAt() < PRICE_TTL_MS
                        ? cached
                        : new CachedPrice(System.currentTimeMillis(), fetchStockPrice(k))
        ).body();
    }

    private Map<String, Object> fetchStockPrice(String code) {
        getAccessToken();

        HttpHeaders headers = new HttpHeaders();
        headers.set("authorization", "Bearer " + accessToken);
        headers.set("appKey", appKey);
        headers.set("appSecret", appSecret);
        headers.set("tr_id", "FHKST01010100");
        headers.set("custtype", "P");

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(baseUrl + "/uapi/domestic-stock/v1/quotations/inquire-price")
                .queryParam("FID_COND_MRKT_DIV_CODE", "J")
                .queryParam("FID_INPUT_ISCD", code);

        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Map> response = restTemplate.exchange(builder.toUriString(), HttpMethod.GET, entity, Map.class);

        return response.getBody();
    }

    public void refreshAccessToken() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, String> body = Map.of(
                "grant_type", "client_credentials",
                "appkey", appKey,
                "appsecret", appSecret
        );

        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
        ResponseEntity<Map> response = restTemplate.postForEntity(baseUrl + "/oauth2/tokenP", request, Map.class);

        Map responseBody = response.getBody();
        accessToken = (String) responseBody.get("access_token");

        log.info("🔄 토큰 갱신 완료: {}", accessToken != null ? "성공" : "실패");
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getAppKey() {
        return appKey;
    }

    public String getAppSecret() {
        return appSecret;
    }
}
