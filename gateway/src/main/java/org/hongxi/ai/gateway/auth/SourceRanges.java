package org.hongxi.ai.gateway.auth;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 令牌来源地址的匹配（R17）：CIDR 与裸地址两种写法，只支持 IPv4 的 CIDR 段。
 *
 * <p><b>解析不了的规则一律当"不匹配"，并且打一条 WARN。</b>另一条路是当"不限制"，那会让一个
 * 打错的字段的实际效果变成把门打开——治理类配置里最不该出现的就是这种方向性错误。宁可锁死并在
 * 日志里喊，也不静默放行。
 *
 * <p>IPv6 只支持等值写法（{@code ::1}），带前缀长度的 IPv6 会按上面的规则拒绝并告警。真要按段
 * 限 IPv6 来源，得先定下规范化的边界（同样的地址有十种写法），别在网关里顺手猜。
 */
public final class SourceRanges {

    private static final Log log = LogFactory.getLog(SourceRanges.class);

    private SourceRanges() {
    }

    /** 空列表表示不限制。任一条命中即通过。 */
    public static boolean allows(List<String> rules, String remoteAddress) {
        if (rules == null || rules.isEmpty()) {
            return true;
        }
        if (remoteAddress == null || remoteAddress.isBlank()) {
            return false;
        }
        for (String rule : rules) {
            if (matches(rule, remoteAddress)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(String rule, String remoteAddress) {
        if (rule == null || rule.isBlank()) {
            log.warn("令牌来源规则里有一条是空的，按不匹配处理");
            return false;
        }
        String trimmed = rule.trim();
        int slash = trimmed.indexOf('/');
        try {
            if (slash < 0) {
                return exact(trimmed, remoteAddress);
            }
            return cidr(trimmed.substring(0, slash), trimmed.substring(slash + 1), remoteAddress);
        }
        catch (RuntimeException | UnknownHostException malformed) {
            log.warn("令牌来源规则 \"" + rule + "\" 解析不了，按不匹配处理（宁可锁死也不放行）");
            return false;
        }
    }

    private static boolean exact(String rule, String remoteAddress) throws UnknownHostException {
        byte[] expected = InetAddress.getByName(rule).getAddress();
        byte[] actual = InetAddress.getByName(remoteAddress).getAddress();
        return java.util.Arrays.equals(expected, actual);
    }

    private static boolean cidr(String network, String prefixText, String remoteAddress) throws UnknownHostException {
        byte[] base = InetAddress.getByName(network).getAddress();
        if (base.length != 4) {
            throw new IllegalArgumentException("只有 IPv4 支持 CIDR 段，IPv6 请写裸地址");
        }
        int bits = Integer.parseInt(prefixText);
        if (bits < 0 || bits > 32) {
            throw new IllegalArgumentException("IPv4 前缀长度必须在 0..32之间，收到 " + bits);
        }
        byte[] actual = InetAddress.getByName(remoteAddress).getAddress();
        if (actual.length != 4) {
            return false;
        }
        int remaining = bits;
        for (int i = 0; i < 4; i++) {
            if (remaining >= 8) {
                if (base[i] != actual[i]) {
                    return false;
                }
                remaining -= 8;
            }
            else if (remaining > 0) {
                int mask = (0xFF << (8 - remaining)) & 0xFF;
                if ((base[i] & mask) != (actual[i] & mask)) {
                    return false;
                }
                remaining = 0;
            }
        }
        return true;
    }
}
