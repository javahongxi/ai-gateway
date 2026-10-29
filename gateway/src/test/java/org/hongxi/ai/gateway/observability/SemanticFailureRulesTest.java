package org.hongxi.ai.gateway.observability;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.hongxi.ai.gateway.observability.ResponseObservationFilter.SemanticFailure;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R14 的判定规则，纯函数级：不起上下文，直接把正文喂进去。
 *
 * <p>这里最重要的几条是<b>不该命中</b>的情况：模型的回答里出现 "quota exceeded" 这种字是常态，
 * 一旦判定去看自由文本，正常调用会被记成厂商故障、进而无谓地打开熔断。误判的代价比漏判大，所以
 * 规则只认厂商放在顶层结构化字段上的错误码。
 */
class SemanticFailureRulesTest {

    private static SemanticFailure detect(String body, String... indicators) {
        return ResponseObservationFilter.detectSemanticFailure(body, List.of(indicators));
    }

    @Test
    void openAiStyleErrorBodyIsStructural() {
        SemanticFailure hit = detect(StubUpstream.ERROR_BODY);

        assertThat(hit).isNotNull();
        assertThat(hit.kind()).isEqualTo("structural");
        assertThat(hit.detail()).isEqualTo("rate_limit_exceeded");
    }

    /** 顶层 code 这条要求厂商先声明：不声明就不判，避免把某家自己的字段当成通用规则。 */
    @Test
    void topLevelCodeNeedsAnIndicator() {
        assertThat(detect(StubUpstream.TOP_LEVEL_CODE_BODY, "Throttling.AllocationQuota"))
                .isNotNull()
                .extracting(SemanticFailure::kind, SemanticFailure::detail)
                .containsExactly("indicator", "Throttling.AllocationQuota");
        assertThat(detect(StubUpstream.TOP_LEVEL_CODE_BODY))
                .as("没声明这个码的厂商不该命中")
                .isNull();
        assertThat(detect(StubUpstream.TOP_LEVEL_CODE_BODY, "SomeOtherCode"))
                .isNull();
    }

    /** 等值比较，大小写无关；不做前缀或包含匹配，免得 "ok" 命中 "not-ok" 这种笑话。 */
    @Test
    void indicatorMatchingIsCaseInsensitiveButExact() {
        assertThat(detect("{\"code\":\"throttling.allocationquota\"}", "Throttling.AllocationQuota")).isNotNull();
        assertThat(detect("{\"code\":\"Throttling.AllocationQuota.Extra\"}", "Throttling.AllocationQuota")).isNull();
    }

    @Test
    void successfulResponsesAreNeverSemanticFailures() {
        String success = "{\"id\":\"chatcmpl-1\",\"choices\":[{\"message\":{\"role\":\"assistant\","
                + "\"content\":\"your quota exceeded, buy more\"}}]," + StubUpstream.USAGE_JSON + "}";

        assertThat(detect(success, "Throttling.AllocationQuota"))
                .as("模型回答里的自由文本不是失败信号：有 choices 就是正常响应")
                .isNull();
        assertThat(detect("{\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":0}}")).isNull();
    }

    /** 有 choices 就不算错误体，哪怕它同时带了 error 字段（有些厂商会两个都塞）。 */
    @Test
    void choicesWinOverErrorSoPartialFailuresAreNotCounted() {
        assertThat(detect("{\"choices\":[{\"message\":{\"content\":\"hi\"}}],"
                + "\"error\":{\"code\":\"rate_limit_exceeded\"}}")).isNull();
    }

    @Test
    void streamFramesAreCheckedToo() {
        assertThat(detect("data: " + StubUpstream.ERROR_BODY + "\n\ndata: [DONE]\n\n"))
                .isNotNull()
                .extracting(SemanticFailure::kind)
                .isEqualTo("structural");
    }

    @Test
    void unparsableOrEmptyBodiesAreNotFailures() {
        assertThat(detect("")).isNull();
        assertThat(detect("{")).isNull();
        assertThat(detect("[DONE]")).isNull();
        // 采不到就什么都不做：漏判由后面的错误率兜住，误判没有兜底
        assertThat(detect("<html>502 Bad Gateway</html>")).isNull();
    }

    /** usage 与语义判定互不干扰：错误体没有 usage，扫出来是 absent 而不是失败之外的第三种东西。 */
    @Test
    void errorBodyCarriesNoUsage() {
        assertThat(ResponseObservationFilter.extract(StubUpstream.ERROR_BODY)).isNull();
        assertThat(ResponseObservationFilter.extract(StubUpstream.TOP_LEVEL_CODE_BODY)).isNull();
    }
}
