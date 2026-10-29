package org.hongxi.ai.gateway.resilience;

import org.junit.jupiter.api.Test;

import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 取余额是纯函数，值得单独钉：厂商把数字写成字符串、把值嵌在下一层、或者干脆改了字段名，都必须在
 * 结果计数里长得不一样，而不是统统变成 0。
 */
class ProviderBalanceParsingTest {

    @Test
    void readsStringNumbersAndNestedPaths() {
        assertThat(ProviderBalanceProbe.value(StubUpstream.BALANCE_BODY, "balance")).isEqualTo(3.5);
        assertThat(ProviderBalanceProbe.value(StubUpstream.BALANCE_BODY, "data.remaining")).isEqualTo(12.5);
    }

    @Test
    void anythingThatIsNotANumberIsNoReading() {
        assertThat(ProviderBalanceProbe.value(StubUpstream.BALANCE_BODY, "data.nope")).isNull();
        assertThat(ProviderBalanceProbe.value(StubUpstream.BALANCE_BODY, "currency"))
                .as("单位不是余额，别把字符串当数")
                .isNull();
        assertThat(ProviderBalanceProbe.value("{\"balance\":\"abc\"}", "balance")).isNull();
        assertThat(ProviderBalanceProbe.value("{", "balance")).isNull();
        assertThat(ProviderBalanceProbe.value("", "balance")).isNull();
        assertThat(ProviderBalanceProbe.value(StubUpstream.BALANCE_BODY, "")).isNull();
    }
}
