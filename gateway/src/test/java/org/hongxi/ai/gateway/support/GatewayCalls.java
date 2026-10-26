package org.hongxi.ai.gateway.support;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * 测试用的网关客户端：把「发一次请求并记录客户端按什么顺序看到什么」这件事收在一处。
 *
 * <p>两个测试类都要它——一个测超时与流式语义，一个测看门狗——所以从测试类里提出来。
 * 观测点是分块的到达时刻，因为「有没有被缓冲」只能靠时刻序列判断，看最终字符串是看不出来的。
 */
public final class GatewayCalls {

    private final WebClient client;
    private final String requestBody;

    public GatewayCalls(String baseUrl, String requestBody) {
        // 直接 WebClient.create：Boot 4 把 WebClient 的自动配置拆进了独立模块
        // spring-boot-webclient，网关的依赖树里没有它，WebClient.Builder 这个 bean 不存在；
        // WebClient 类本身来自 spring-webflux，不必为了一个 Builder 加依赖
        this.client = WebClient.create(baseUrl);
        this.requestBody = requestBody;
    }

    /** 发一次并等它结束（含失败）。{@code authorization} 为 null 表示不带 Authorization。 */
    public Outcome call(String path, String authorization) {
        long started = System.nanoTime();
        AtomicInteger status = new AtomicInteger();
        HttpHeaders responseHeaders = new HttpHeaders();
        List<String> pieces = new CopyOnWriteArrayList<>();
        List<Long> arrivals = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            body(path, authorization, status, responseHeaders)
                    .doOnNext(buffer -> {
                        pieces.add(read(buffer));
                        arrivals.add(elapsedSince(started));
                    })
                    .then()
                    .block(Duration.ofSeconds(20));
        }
        catch (RuntimeException timeoutOrAbort) {
            failure.set(timeoutOrAbort);
        }
        return new Outcome(status.get(), String.join("", pieces), elapsedSince(started), List.copyOf(arrivals),
                failure.get(), HttpHeaders.readOnlyHttpHeaders(responseHeaders));
    }

    /** 发一次但不等它结束，用来观察「还挂着的那条流」现在怎么样了。 */
    public LiveCall start(String path, String authorization) {
        AtomicInteger status = new AtomicInteger();
        return new LiveCall(status, body(path, authorization, status, new HttpHeaders()));
    }

    private Flux<DataBuffer> body(String path, String authorization, AtomicInteger status,
            HttpHeaders capturedHeaders) {
        return client.post()
                .uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .headers(headers -> {
                    if (authorization != null) {
                        headers.set(HttpHeaders.AUTHORIZATION, authorization);
                    }
                })
                .bodyValue(requestBody)
                .exchangeToFlux(response -> {
                    status.set(response.statusCode().value());
                    capturedHeaders.putAll(response.headers().asHttpHeaders());
                    return response.bodyToFlux(DataBuffer.class);
                });
    }

    private static long elapsedSince(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private static String read(DataBuffer buffer) {
        String text = buffer.toString(StandardCharsets.UTF_8);
        DataBufferUtils.release(buffer);
        return text;
    }

    /**
     * 把 SSE 里的 {@code delta.content} 拼回原文。手写解析够用且不引依赖：桩的输出形状固定。
     */
    public static String reassembleDeltas(String sse) {
        StringBuilder text = new StringBuilder();
        for (String line : sse.split("\n")) {
            int marker = line.indexOf("\"content\":\"");
            if (marker < 0) {
                continue;
            }
            int from = marker + "\"content\":\"".length();
            int to = line.indexOf('"', from);
            if (to > from) {
                text.append(line, from, to);
            }
        }
        return text.toString();
    }

    /** 一次请求的完整观测结果。 */
    public record Outcome(int status, String body, long elapsed, List<Long> arrivals, Throwable failure,
            HttpHeaders headers) {
    }

    /** 一次已经发出的请求，能在它还在跑的时候观察状态码、已到达的分块与到达时刻。 */
    public static final class LiveCall {

        private final AtomicInteger status;
        private final List<String> pieces = new CopyOnWriteArrayList<>();
        private final List<Long> arrivals = new CopyOnWriteArrayList<>();
        private final AtomicBoolean terminated = new AtomicBoolean();
        private final AtomicLong terminatedAt = new AtomicLong(-1L);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Disposable subscription;

        private LiveCall(AtomicInteger status, Flux<DataBuffer> response) {
            this.status = status;
            long started = System.nanoTime();
            this.subscription = response
                    .doOnNext(buffer -> {
                        pieces.add(read(buffer));
                        arrivals.add(elapsedSince(started));
                    })
                    .then()
                    .subscribe(ignored -> {
                    }, error -> {
                        failure.set(error);
                        markTerminated(started);
                    }, () -> markTerminated(started));
        }

        private void markTerminated(long startedNanos) {
            terminatedAt.set(elapsedSince(startedNanos));
            terminated.set(true);
        }

        /** 等这么久，要么流结束，要么耐心用完。 */
        public void awaitQuietly(Duration patience) {
            long deadline = System.nanoTime() + patience.toNanos();
            while (System.nanoTime() < deadline && !terminated.get()) {
                try {
                    Thread.sleep(20);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /**
         * 流在第几毫秒结束的（相对发起请求）；还在跑返回 -1。分块到达的时刻说明不了
         * 「什么时候被切断」，只有这里能给那个数。
         */
        public long terminatedAfterMillis() {
            return terminated.get() ? terminatedAt.get() : -1L;
        }

        public int status() {
            return status.get();
        }

        public String body() {
            return String.join("", pieces);
        }

        public List<Long> arrivals() {
            return List.copyOf(arrivals);
        }

        public boolean terminated() {
            return terminated.get();
        }

        /** 流是怎么结束的：{@code null} 表示正常结束或被取消。 */
        public Throwable failure() {
            return failure.get();
        }

        public void cancel() {
            subscription.dispose();
        }
    }
}
