package org.hongxi.ai.gateway.resilience;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R18：把厂商账户里的余额/配额读成一个 gauge。
 *
 * <p><b>只报不治。</b>这件事和熔断、路由、密钥池都没有关系：余额低到快要用完是一个<b>运维信号</b>，
 * 不是一次调用失败。所以本类不改任何状态——不碰熔断、不动冷却、不影响选路。真有"余额为零就把这家
 * 摘掉"的需求，那是路由策略该长的东西，不该藏在一个读数字的地方。
 *
 * <p><b>读不到就说读不到。</b>响应里没有那个字段、字段不是数、端点回 4xx/5xx、连不上，分成四种
 * 结果计数（{@code missing-field|http-error|error}），而不是把 gauge 悄悄写成 0——"没采到"和
 * "余额为零"是两个完全不同的告警条件，混起来会让人在半夜被叫起来发现是字段改名了。
 *
 * <p><b>各家形状差异交给配置。</b>路径（{@code balance-path}）与取值位置（{@code balance-field}，
 * 点号分层）都由声明给出；厂商把数字写成字符串（{@code "3.5"}）这种情况也取。没声明的厂商一次请求
 * 都不发。单位（{@code balance-currency}）进 tag：不同单位的余额放在同一个看板上相加是错的。
 *
 * <p>与 {@link ProviderHealthProbe}（R15）是两个类而不是一个带开关的类：两者成功之后做的事完全不
 * 一样（一个收熔断、一个只记数），合并只会得到一个靠 flag 分支的类。共用的是<b>约定</b>——独立
 * {@link WebClient} 绕开过滤器链、间隔大于初始延迟以免一起来就打、开关只管定时入口而直接调用始终
 * 可用于测试与运维脚本。
 */
@Component
public class ProviderBalanceProbe {

    /** 读取结果分布：{@code ok|missing-field|http-error|error|skipped}。 */
    public static final String BALANCE_SAMPLES_METRIC = "ai.gateway.provider.balance.samples";

    /** 厂商账户余额，按厂商与单位两条 tag。 */
    public static final String BALANCE_GAUGE = "ai.gateway.provider.balance";

    private static final Logger log = LoggerFactory.getLogger(ProviderBalanceProbe.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AiGatewayProperties properties;

    private final MeterRegistry registry;

    /** 每个厂商一份读数值，gauge 绑在它上面。没读到之前不放 gauge，免得把"没采"显示成 0。 */
    private final Map<String, AtomicReference<Double>> latest = new ConcurrentHashMap<>();

    public ProviderBalanceProbe(AiGatewayProperties properties, MeterRegistry registry) {
        this.properties = properties;
        this.registry = registry;
    }

    /** 定时入口。初始延迟等于间隔，否则应用一启动就先打一轮外部调用。 */
    @Scheduled(fixedDelayString = "${ai-gateway.balance.interval:10m}",
            initialDelayString = "${ai-gateway.balance.interval:10m}")
    public void scheduledRead() {
        if (properties.getBalance().isEnabled()) {
            readBalances();
        }
    }

    /**
     * 读一轮，返回真正发出请求的厂商数。直接公开给测试与运维脚本：真实时间不参与断言，
     * 与 R15 同一套做法。
     */
    public int readBalances() {
        int sent = 0;
        for (Map.Entry<String, AiGatewayProperties.Provider> entry : properties.getProviders().entrySet()) {
            String alias = entry.getKey();
            AiGatewayProperties.Provider provider = entry.getValue();
            if (!StringUtils.hasText(provider.getBalancePath()) || !StringUtils.hasText(provider.getBalanceField())) {
                // 没声明就是不看这家。不发"skipped"计数也不发请求，免得没配的东西在看板上占位置
                continue;
            }
            sent++;
            readOne(alias, provider);
        }
        return sent;
    }

    private void readOne(String alias, AiGatewayProperties.Provider provider) {
        List<String> keys = provider.effectiveKeys();
        if (keys.isEmpty()) {
            sample("error", alias);
            log.warn("厂商 '{}' 配了 balance-path 但没有可用凭证，这次读不到余额", alias);
            return;
        }
        String currency = StringUtils.hasText(provider.getBalanceCurrency()) ? provider.getBalanceCurrency() : "credits";
        try {
            // 余额是账户级的，用池子里第一把 key 就够，不参与轮转（否则每读一次余额就把轮换的相位推一格）
            ReadResult result = WebClient.create(provider.getBaseUrl())
                    .get()
                    .uri(provider.getBalancePath())
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + keys.get(0))
                    .exchangeToMono(response -> {
                        int status = response.statusCode().value();
                        if (status < 200 || status >= 300) {
                            return response.releaseBody().then(Mono.just(new ReadResult(status, null)));
                        }
                        return response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .map(body -> new ReadResult(status, value(body, provider.getBalanceField())));
                    })
                    .block(properties.getBalance().getTimeout());

            if (result.status() < 200 || result.status() >= 300) {
                sample("http-error", alias);
                log.warn("厂商 '{}' 的余额端点返回 {}，gauge 保持上一次的值", alias, result.status());
                return;
            }
            if (result.value() == null) {
                sample("missing-field", alias);
                log.warn("厂商 '{}' 的余额响应里找不到字段 '{}'（改名？形状变了？），gauge 保持上一次的值",
                        alias, provider.getBalanceField());
                return;
            }
            latestOf(alias, currency).set(result.value());
            sample("ok", alias);
        }
        catch (RuntimeException unreachable) {
            sample("error", alias);
            log.warn("读厂商 '{}' 的余额失败：{}", alias, unreachable.toString());
        }
    }

    private record ReadResult(int status, Double value) {
    }

    /**
     * 按点号路径取一个数。数字与数字字符串都算（厂商常把 {@code 3.5} 写成 {@code "3.5"}）；
     * 路径不通、不是数、JSON 解析失败都返回 {@code null}，由调用方记成 missing-field。
     */
    static Double value(String body, String field) {
        if (!StringUtils.hasText(body) || !StringUtils.hasText(field)) {
            return null;
        }
        JsonNode node;
        try {
            node = MAPPER.readTree(body);
        }
        catch (Exception unparsable) {
            return null;
        }
        for (String segment : field.split("\\.")) {
            node = node.path(segment);
        }
        if (node.isNumber()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            try {
                return Double.valueOf(node.asText().trim());
            }
            catch (NumberFormatException notANumber) {
                return null;
            }
        }
        return null;
    }

    private AtomicReference<Double> latestOf(String alias, String currency) {
        return latest.computeIfAbsent(alias + "|" + currency, key -> {
            AtomicReference<Double> holder = new AtomicReference<>(Double.NaN);
            Gauge.builder(BALANCE_GAUGE, holder, ProviderBalanceProbe::read)
                    .description("厂商账户余额，按厂商与单位；没读到之前没有这条时间线")
                    .tag("provider", alias)
                    .tag("currency", currency)
                    .register(registry);
            return holder;
        });
    }

    private static double read(AtomicReference<Double> holder) {
        Double value = holder.get();
        return value == null ? Double.NaN : value;
    }

    private void sample(String outcome, String alias) {
        Counter.builder(BALANCE_SAMPLES_METRIC)
                .description("余额读取结果分布")
                .tag("outcome", outcome)
                .tag("provider", alias)
                .register(registry)
                .increment();
    }
}
