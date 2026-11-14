package org.hongxi.ai.gateway.routing;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R23 预扣额的纯函数单测：不起上下文，直接喂字节给 {@link RequestBodyModels#maxTokensOf}。
 * 取舍见决策 D19——只预扣 {@code max_tokens}，读不到就退到保守默认值，宁可多预扣不可少预扣。
 */
class TokenReservationTest {

    private static byte[] bytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void positiveMaxTokensIsTheReservation() {
        assertThat(RequestBodyModels.maxTokensOf(bytes("{\"model\":\"m\",\"max_tokens\":60}"), 1024))
                .isEqualTo(60);
    }

    @Test
    void missingMaxTokensFallsBackToTheDefault() {
        assertThat(RequestBodyModels.maxTokensOf(bytes("{\"model\":\"m\"}"), 1024)).isEqualTo(1024);
    }

    /** 非正的 max_tokens 不是有界预扣额，退到默认——0 会让预扣形同虚设。 */
    @Test
    void nonPositiveMaxTokensFallsBack() {
        assertThat(RequestBodyModels.maxTokensOf(bytes("{\"max_tokens\":0}"), 1024)).isEqualTo(1024);
        assertThat(RequestBodyModels.maxTokensOf(bytes("{\"max_tokens\":-5}"), 1024)).isEqualTo(1024);
    }

    @Test
    void malformedOrEmptyBodyFallsBack() {
        assertThat(RequestBodyModels.maxTokensOf(bytes("not json"), 1024)).isEqualTo(1024);
        assertThat(RequestBodyModels.maxTokensOf(bytes(""), 1024)).isEqualTo(1024);
        assertThat(RequestBodyModels.maxTokensOf(null, 1024)).isEqualTo(1024);
    }

    /** 字符串形的 max_tokens 不是数字，退到默认（厂商要的是数字，网关不替它猜）。 */
    @Test
    void textualMaxTokensFallsBack() {
        assertThat(RequestBodyModels.maxTokensOf(bytes("{\"max_tokens\":\"60\"}"), 1024)).isEqualTo(1024);
    }
}