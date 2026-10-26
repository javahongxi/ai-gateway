package org.hongxi.ai.gateway.resilience;

import java.util.concurrent.TimeUnit;

import io.netty.handler.timeout.ReadTimeoutHandler;
import reactor.netty.http.client.HttpClient;

import org.springframework.cloud.gateway.config.HttpClientCustomizer;
import org.springframework.stereotype.Component;

import org.hongxi.ai.gateway.config.UpstreamProperties;

/**
 * R11：上游流中途卡住时的空闲保护。
 *
 * <p>为什么需要它：路由 metadata 的 {@code response-timeout} 管不到这一段。被它包住的
 * {@code responseFlux} 来自 {@code responseConnection(...)}，在厂商响应头到达时就已经完成，
 * 之后正文由 NettyWriteResponseFilter 直接从连接里取，中间没有任何超时。厂商「开始流式然后不吐字」
 * 时，这条上游连接会一直挂着，网关的线程与连接被占住，客户端也等不到结束——实测由
 * {@code GatewayChatFlowTest#midStreamStallIsNotCutOffByResponseTimeout} 钉住。
 *
 * <p>做法是给上游通道加一个 Netty {@link ReadTimeoutHandler}：连续 {@code idleTimeout} 没有读到
 * 任何字节就抛 {@code ReadTimeoutException} 并关连接，正文流随之终止。它是**空闲**计时，只要厂商
 * 持续吐 token 就永远不会触发，所以长回答不受影响。
 *
 * <p>粒度是全局而非 per-route：{@code HttpClientCustomizer} 是在构造网关那个唯一 HttpClient 时
 * 套上去的（{@code HttpClientFactory#applyCustomizers}），拿不到具体路由。要按厂商给不同预算，
 * 得等 per-route HttpClient 或改成在过滤器链里自己包正文流——先用全局值把「无限挂死」这个最坏的
 * 性质消掉。
 */
@Component
public class UpstreamIdleTimeoutCustomizer implements HttpClientCustomizer {

    private final UpstreamProperties properties;

    public UpstreamIdleTimeoutCustomizer(UpstreamProperties properties) {
        this.properties = properties;
    }

    @Override
    public HttpClient customize(HttpClient httpClient) {
        if (!properties.idleTimeoutEnabled()) {
            return httpClient;
        }
        long millis = properties.idleTimeout().toMillis();
        // 每个通道一份 handler：ReadTimeoutHandler 带计时状态，不能共享
        return httpClient.doOnChannelInit((observer, channel, remoteAddress) ->
                channel.pipeline().addFirst(new ReadTimeoutHandler(millis, TimeUnit.MILLISECONDS)));
    }
}
