package org.hongxi.ai.gateway.observability;

import org.junit.jupiter.api.Test;

import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R12 的扫描规则本身：同一段 usage，三种投递形状都要给出同一组数。
 *
 * <p>这是端到端用例的最小组件版本——端到端挂掉时，这里能立刻分清是"扫描规则错了"还是
 * "装饰器没看到字节"。
 */
class UsageExtractionTest {

    private static final String USAGE = StubUpstream.USAGE_JSON;

    private static ResponseObservationFilter.Usage parse(String record) {
        return ResponseObservationFilter.extract(record);
    }

    @Test
    void nonStreamBodyIsOneRecord() {
        String body = "{\"id\":\"chatcmpl-1\",\"choices\":[{\"message\":{\"content\":\"pong\"}}]," + USAGE + "}";

        assertThat(parse(body)).isEqualTo(new ResponseObservationFilter.Usage(11, 7, 4, 3));
    }

    @Test
    void usageOnTheFinishChunkIsFound() {
        String body = "data: {\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\n"
                + "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]," + USAGE + "}\n\n"
                + "data: [DONE]\n\n";

        assertThat(parse(body)).isEqualTo(new ResponseObservationFilter.Usage(11, 7, 4, 3));
    }

    /** DashScope 的形状：usage 不在 finish 那个块上，而在它后面多出来的一个块里。 */
    @Test
    void usageInAnExtraChunkAfterFinishIsFound() {
        String body = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
                + "data: {\"choices\":[]," + USAGE + "}\n\n"
                + "data: [DONE]\n\n";

        assertThat(parse(body)).isEqualTo(new ResponseObservationFilter.Usage(11, 7, 4, 3));
    }

    @Test
    void noUsageAnywhereIsReportedAsAbsent() {
        assertThat(parse("data: {\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\ndata: [DONE]\n\n")).isNull();
        assertThat(parse("")).isNull();
        // 帧被拆坏或不是 JSON 时不抛异常，只是采不到
        assertThat(parse("data: {\"usage\":{\"completion_tokens\":")).isNull();
    }

    /** 只有 prompt_tokens 而没有 completion_tokens 的不算采到：那条规则要求见到 completion_tokens。 */
    @Test
    void partialUsageIsNotTreatedAsASample() {
        assertThat(parse("{\"usage\":{\"prompt_tokens\":5}}")).isNull();
    }
}
