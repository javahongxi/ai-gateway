package org.hongxi.ai.gateway.auth;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R17 的来源地址匹配。核心是<b>方向</b>：解析不了的规则按"不匹配"处理，因为治理类配置里一个打错的
 * 字段最不该产生的效果是把门打开。
 */
class SourceRangesTest {

    @Test
    void emptyRuleSetMeansNoRestriction() {
        assertThat(SourceRanges.allows(List.of(), "203.0.113.7")).isTrue();
        assertThat(SourceRanges.allows(null, "203.0.113.7")).isTrue();
    }

    @Test
    void cidrCoversTheWholeRange() {
        List<String> rules = List.of("10.20.30.0/24");

        assertThat(SourceRanges.allows(rules, "10.20.30.1")).isTrue();
        assertThat(SourceRanges.allows(rules, "10.20.30.255")).isTrue();
        assertThat(SourceRanges.allows(rules, "10.20.31.1")).as("跨过一个位就出局").isFalse();
        assertThat(SourceRanges.allows(rules, "10.19.30.1")).isFalse();
    }

    @Test
    void prefixBoundariesBehave() {
        assertThat(SourceRanges.allows(List.of("0.0.0.0/0"), "203.0.113.7")).isTrue();
        assertThat(SourceRanges.allows(List.of("203.0.113.7/32"), "203.0.113.7")).isTrue();
        assertThat(SourceRanges.allows(List.of("203.0.113.7/32"), "203.0.113.8")).isFalse();
        // /4 只比第一个 octet 的高四位：192=1100_0000、203=1100_1011 同属 1100，208=1101_0000 不是。
        // （这条一开始写反了，是跑出来才发现的——掩码算术值得把二进制写在断言里。）
        assertThat(SourceRanges.allows(List.of("192.0.2.0/4"), "203.0.113.7")).isTrue();
        assertThat(SourceRanges.allows(List.of("192.0.2.0/4"), "208.0.0.1"))
                .as("高四位是 1101，不在 192.0.2.0/4 里")
                .isFalse();
    }

    @Test
    void bareAddressIsAnExactMatch() {
        assertThat(SourceRanges.allows(List.of("127.0.0.1"), "127.0.0.1")).isTrue();
        assertThat(SourceRanges.allows(List.of("127.0.0.1"), "127.0.0.2")).isFalse();
        assertThat(SourceRanges.allows(List.of("::1"), "0:0:0:0:0:0:0:1"))
                .as("同一个回环地址的两种写法要能对上")
                .isTrue();
    }

    /** 打错的字段不能变成放行。 */
    @Test
    void malformedRulesFailClosed() {
        assertThat(SourceRanges.allows(List.of("not-an-ip/24"), "127.0.0.1")).isFalse();
        assertThat(SourceRanges.allows(List.of("10.0.0.0/33"), "10.0.0.1")).isFalse();
        assertThat(SourceRanges.allows(List.of(""), "127.0.0.1")).isFalse();
        assertThat(SourceRanges.allows(List.of("2001:db8::/32"), "2001:db8::1"))
                .as("IPv6 段没实现就明确不按放行处理")
                .isFalse();
    }

    @Test
    void missingRemoteAddressFailsClosedWhenRulesExist() {
        assertThat(SourceRanges.allows(List.of("10.0.0.0/8"), null)).isFalse();
        assertThat(SourceRanges.allows(List.of("10.0.0.0/8"), "  ")).isFalse();
        assertThat(SourceRanges.allows(List.of(), null)).as("没规则就等于不限制").isTrue();
    }

    /** 一条命中即通过：多段与裸地址混着写是常态。 */
    @Test
    void anyMatchingRuleIsEnough() {
        assertThat(SourceRanges.allows(List.of("10.0.0.0/8", "192.168.1.5", "127.0.0.0/8"), "127.0.0.1"))
                .isTrue();
    }
}
