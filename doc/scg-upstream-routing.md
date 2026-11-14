# SCG 上游调用机制：内置 NettyRoutingFilter 与自定义接管

承接 `threading-model.md`（那里讲了整条过滤器链跑在 reactor-netty event loop 上）。这份文档回答
三个问题：网关到上游的 HTTP 调用到底由谁发起、如果想自己接管这个调用该怎么做、接管后内置的那个
Filter 还走不走。纯框架知识点，示例均为泛化命名。

---

## 一、先认清内置的两个 Filter

真正发起上游 HTTP 调用的是 **`NettyRoutingFilter`**，不是 `RouteToRequestUrlFilter`：

- `RouteToRequestUrlFilter`（order = `10000`）先把路由 URI 与请求拼成 `GATEWAY_REQUEST_URL_ATTR`。
- `NettyRoutingFilter`（`GlobalFilter`，order = `LOWEST_PRECEDENCE`，链尾）读这个属性，用
  reactor-netty `HttpClient` 真正发调用。它开头有守卫：
  `isAlreadyRouted(exchange) || scheme 非 http/https` → 直接 `chain.filter(exchange)` 跳过。
  调用后 `setAlreadyRouted(exchange)`，并把上游响应/连接塞进 `CLIENT_RESPONSE_ATTR` /
  `CLIENT_RESPONSE_CONN_ATTR`。
- **`NettyWriteResponseFilter`**（order = `-1`）负责把上游响应体写回客户端：从 `CLIENT_RESPONSE_ATTR`
  取连接，包在 `chain.filter(exchange).then(...)` 里，所以「写响应」发生在整条链返回之后。

**三个机关决定内置是否还走**：
1. `gatewayAlreadyRouted` 标记（`ServerWebExchangeUtils.setAlreadyRouted/isAlreadyRouted`）；
2. URI scheme 判断（`NettyRoutingFilter` 只认 http/https）；
3. `CLIENT_RESPONSE_ATTR` 有没有设（决定 `NettyWriteResponseFilter` 是否帮你写响应体）。

---

## 二、自己接管调用的三种做法

### 做法一：`setAlreadyRouted` —— 内置还在链上，但空走
最通用、最不侵入。写一个 order 在 `NettyRoutingFilter` 之前的 `GlobalFilter`，自己发起调用、
自己写响应，然后：

```java
ServerWebExchangeUtils.setAlreadyRouted(exchange); // gatewayAlreadyRouted = true
```

`NettyRoutingFilter` 执行到它时 `isAlreadyRouted == true` → `return chain.filter(exchange)`，
不发起调用。
- 内置 Filter **仍被调用，但自我跳过（空走）**，不是被移除。
- 你若没设 `CLIENT_RESPONSE_ATTR`，`NettyWriteResponseFilter` 也空转。

### 做法二：`extends NettyRoutingFilter` 顶替 Bean —— 内置 Bean 被取代
SCG 自动配置的 `nettyRoutingFilter` Bean 带 `@ConditionalOnMissingBean`。自定义一个
`@Component` 子类继承 `NettyRoutingFilter`（IS-A 关系），自动配置的那个就**退避**，链上只剩你的子类：

```java
@Component
public class CustomRoutingFilter extends NettyRoutingFilter {
    // 构造器签名与自动配置的 nettyRoutingFilter Bean 方法一致
    public CustomRoutingFilter(HttpClient httpClient,
            ObjectProvider<List<HttpHeadersFilter>> headersFilters,
            HttpClientProperties properties) {
        super(httpClient, headersFilters, properties);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // 可读自定义 URL 属性：没设就 pass-through，设了才自己调（可用自定义请求体）
        // 自己调完仍 setAlreadyRouted + 设 CLIENT_RESPONSE_ATTR，复用 NettyWriteResponseFilter 写回
        ...
    }
}
```
- 此时「内置那个」根本不被创建，链上的路由过滤器就是你的子类。
- 仍设 `CLIENT_RESPONSE_ATTR` 的话，**响应体写回继续复用 `NettyWriteResponseFilter`**。
- 适合「仍是 HTTP，但要定制请求体/URL/调用细节」的场景。

### 做法三：自定义 URI scheme + 协议 GlobalFilter —— 内置因 scheme 不符自我跳过
路由 URI 用非 http/https 的 scheme（如 `myrpc://...`），写一个按 scheme 匹配接管的 `GlobalFilter`：

```java
@Component
public class MyRpcProtocolFilter implements GlobalFilter, Ordered {
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        URI url = exchange.getAttribute(GATEWAY_REQUEST_URL_ATTR);
        if (url == null || !"myrpc".equals(url.getScheme())) {
            return chain.filter(exchange); // scheme 不符，交给别人
        }
        // 自己发起 RPC 调用、自己写响应（不设 CLIENT_RESPONSE_ATTR）
        ...
    }

    @Override
    public int getOrder() {
        return 10000 + 100; // RouteToRequestUrl 之后、NettyRoutingFilter 之前
    }
}
```
- 内置 `NettyRoutingFilter` 仍在链上，但 scheme 非 http/https → **自我跳过**。
- 没设 `CLIENT_RESPONSE_ATTR` → `NettyWriteResponseFilter` 空转。
- 适合把上游换成 TCP/RPC 等**非 HTTP 协议**。

---

## 三、内置 Filter 是否还走（对照表）

| 做法 | `NettyRoutingFilter` | `NettyWriteResponseFilter` |
|---|---|---|
| 一 `setAlreadyRouted` | 在链上、被调用，但 `isAlreadyRouted` 跳过（空走） | 没设 `CLIENT_RESPONSE_ATTR` 则空转 |
| 二 `extends` 顶替 Bean | 内置 Bean 不创建，被你的子类取代 | 子类设了 `CLIENT_RESPONSE_ATTR` → 照常帮你写 |
| 三 自定义 scheme | 在链上，scheme 非 http/https 跳过（空走） | 没设 `CLIENT_RESPONSE_ATTR` 则空转 |

---

## 四、对本项目的意义

ai-gateway 目前走的就是内置 `NettyRoutingFilter` + http/https 路由（见 `threading-model.md` 的
order 链：`RouteToRequestUrl(10000) → NettyWriteResponse`）。将来若想自己接管 LLM 调用：
- **做法一 `setAlreadyRouted` 最不侵入**——不动 Bean、不改 scheme，只在链上插一个过滤器并标记已路由。
- 想换非 HTTP 协议才用做法三；想深度定制 HTTP 调用细节又愿意顶替 Bean 时用做法二。
- 无论哪种，都要想清楚响应体由谁写：设 `CLIENT_RESPONSE_ATTR` 就复用 `NettyWriteResponseFilter`，
  否则自己写、它空转。

---

## 五、一句话总结

内置 `NettyRoutingFilter` 不是「被删掉」，而是靠 **`gatewayAlreadyRouted` 标记 / scheme 判断 /
Bean 顶替** 三种机关让它「不发起调用」。而决定 `NettyWriteResponseFilter` 是否还帮你写响应体的，
是你有没有设 `CLIENT_RESPONSE_ATTR`——设了就复用它的写回逻辑，没设就自己写、它空转。