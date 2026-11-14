# ai-gateway

基于 Spring Cloud Gateway 的大模型网关：对外一个 OpenAI 兼容入口，按配置把请求路由到多家模型厂商。

需求、决策与实测记录见 [docs/requirements.md](docs/requirements.md)。

## 过滤器

请求处理是一条按 `order` **升序**执行的过滤器链。SCG 把 `GlobalFilter`（全局生效）与路由级
`GatewayFilter`（由工厂生产、只挂需要的路由）适配成同一条链统一排序——机制见
[doc/threading-model.md](doc/threading-model.md)，上游调用的接管见
[doc/scg-upstream-routing.md](doc/scg-upstream-routing.md)。下表按执行顺序列出全部过滤器：

| 过滤器类 | 类型 | order | 作用 |
|---|---|---|---|
| `RequestMetricsFilter` | GlobalFilter·自研 | `HIGHEST_PRECEDENCE`（MIN_VALUE） | 按租户+厂商+状态记请求指标，含被鉴权挡掉的；排最前 |
| `ResponseObservationFilter` | GlobalFilter·自研 | `HIGHEST_PRECEDENCE + 1` | 响应侧单一观察者：读 usage、结算 token、冷却 key、记响应指标与访问日志 |
| `TenantAuthFilter` | GlobalFilter·自研 | `HIGHEST_PRECEDENCE + 10` | 租户鉴权（Bearer key→租户）+ 来源地址/到期/模型白名单治理 |
| `FirstTokenMetricsFilter` | GlobalFilter·自研 | `-2` | 记 TTFT（进入网关到首字节交给客户端） |
| `NettyWriteResponseFilter` | GlobalFilter·内置 | `-1` | 把上游响应体写回客户端（响应阶段 `.then()` 才执行） |
| `RetryAfterFilter` | GlobalFilter·自研 | `0` | 给 429 兜底 `Retry-After` 头 |
| `RequestRateLimiter` | GatewayFilter·内置·default | `1` | 租户请求数令牌桶限流（Redis），写 `X-RateLimit-*` 头 |
| `Retry` | GatewayFilter·内置·路由 | `1` | 首字节前请求内重试（R24，默认关） |
| `CircuitBreaker` | GatewayFilter·内置·路由 | `2` | 熔断 + `forward` 改道到备份 |
| `ProviderKeyGatewayFilterFactory` | GatewayFilter·自研·路由 | `3` | 选厂商 key、换 `Authorization`、登记冷却（R16） |
| `ModelMappingGatewayFilterFactory` | GatewayFilter·自研·路由 | `4` | 请求侧改 `model` 名（客户端名→厂商名，D9） |
| `SetPath` | GatewayFilter·内置·路由 | `5` | 改上游路径为厂商 `chat-path` |
| `TokenQuotaFilter` | GlobalFilter·自研 | `95` | 租户 token 预扣（R23，默认关） |
| `ProviderQuotaFilter` | GlobalFilter·自研 | `100` | 厂商配额固定窗口限流（Redis） |
| `RouteToRequestUrlFilter` | GlobalFilter·内置 | `10000` | 拼最终上游 URL（`GATEWAY_REQUEST_URL_ATTR`） |
| `NettyRoutingFilter` | GlobalFilter·内置 | `LOWEST_PRECEDENCE`（MAX_VALUE） | 用 reactor-netty `HttpClient` 真正发起上游 HTTP 调用 |
| `ForwardRoutingFilter` | GlobalFilter·内置 | `LOWEST_PRECEDENCE` | `forward:` 路由本地分发（`/v1/models`、备份改道），与上一行按 scheme 互斥 |

几点说明：

- **order 决定链上位置，不等于「先做完」**：`ResponseObservationFilter`、`FirstTokenMetricsFilter`、
  `NettyWriteResponseFilter` 的主要动作在响应阶段（`.then()`/`doFinally`）才发生，虽然位置靠前。
- **路由级过滤器的 order 由装配位置决定**（第 N 个 = N，见 SCG `RouteDefinitionRouteLocator`）。
  default-filter 先于路由过滤器加载，同 order 时稳定排序靠前（故 `RequestRateLimiter` 在 `Retry` 前）。
  上表按 `Retry`+`CircuitBreaker` 都启用列；关掉某个时后续位置前移，**相对顺序不变**。
- **`CircuitBreaker` 必须在 `ProviderKey`/`ModelMapping`/`SetPath` 之前**（D6.1：改道重投递时
  鉴权不能读到已换进去的厂商 key，否则 401）。
- **默认关**：`Retry`（R24 `max-attempts=0`）默认不挂到路由；`TokenQuotaFilter`（R23 配额 0）仍在链上
  但空转（不读体、不碰 Redis）。

## 跑起来

上游厂商 key 从环境变量读，不进仓库。网关只认这两个变量，各条路由的 `SetRequestHeader` 引用自己的那家：

```
export DASHSCOPE_API_KEY=sk-...
export DEEPSEEK_API_KEY=sk-...
mvn -pl gateway spring-boot:run          # 监听 8088
```

非流式调用，`sk-team-a-local-dev` 是 `application.yml` 里的本地开发租户 key：

```
curl http://127.0.0.1:8088/v1/chat/completions \
  -H "Authorization: Bearer sk-team-a-local-dev" \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen3.7-plus","messages":[{"role":"user","content":"用一个词回答：天空的颜色"}],"max_tokens":8}'
```

```
{"model":"qwen3.7-plus","id":"chatcmpl-913805cc-b3b3-9a09-8ac9-f0a543bc4b44","choices":[{"message":{"content":"蓝色。",...
```

请求体加 `"stream":true` 就是流式，客户端拿到的是厂商原样的 SSE：

```
data: {"model":"qwen3.7-plus","id":"chatcmpl-864da560-...","usage":null,"choices":[...
data: {"choices":[],"usage":{"completion_tokens":49,"completion_tokens_details":{"reasoning_tokens":46,...
data: [DONE]
```

换厂商只改路径前缀，OpenAI SDK 那边就是改 `base_url`：

```
curl http://127.0.0.1:8088/deepseek/v1/chat/completions \
  -H "Authorization: Bearer sk-team-a-local-dev" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"用一个词回答：天空的颜色"}],"max_tokens":128}'
```

```
{"id":"94fae9bb-7c86-48d8-9e9a-c0df134179ff","object":"chat.completion",...,"model":"deepseek-flash","choices":[{"index":0,"message":{"role":"assistant","content":"蓝色"},...
```

回显的 `model` 是厂商自己的别名（请求 `deepseek-v4-flash`，回来写 `deepseek-flash`），网关不改报文所以原样透出。`max_tokens` 给小了会全被思考过程吃掉：`deepseek-v4-flash` 与 `qwen3.7-plus` 都是推理模型，`max_tokens: 8` 时 `content` 是空的，思考占了几十个 token。

没有 key 时请求不会碰到厂商，按 OpenAI 的错误信封返回 401：

```
{"error":{"message":"Missing API key: send an Authorization: Bearer <gateway key> header.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}
```

## 观测

运维面单独端口，不与 8088 的业务端口共用（8088 上 `/actuator/**` 是 404）：

```
curl -s http://127.0.0.1:8089/actuator/prometheus | grep ^ai_gateway_
```

```
ai_gateway_requests_seconds_count{application="gateway",provider="deepseek",route="deepseek-chat",status="200",tenant="team-a"} 1
ai_gateway_requests_seconds_sum{application="gateway",provider="deepseek",route="deepseek-chat",status="200",tenant="team-a"} 2.056642
ai_gateway_ttft_seconds_count{application="gateway",provider="deepseek",route="deepseek-chat",tenant="team-a"} 1
ai_gateway_ttft_seconds_sum{application="gateway",provider="deepseek",route="deepseek-chat",tenant="team-a"} 1.454746
```

`ttft` 是「进入网关到第一个字节交给客户端」，与整条请求的耗时分开记：一次 `max_tokens=200` 的流式回答，首字 1.45 秒、整条 2.06 秒，只看后者会把「多久开始吐字」和「一共吐了多久」混成一个数。

traceId 由 `spring-boot-starter-opentelemetry` 提供，会作为 `traceparent` 传给厂商（厂商侧日志才能与网关对上）。两个坑记在决策 D8：`management.tracing.export.enabled=false` 会连传播一起关掉；WebFlux + OTel bridge 下 MDC 里没有 traceId，所以日志格式没写 `%X{traceId}`。

被鉴权挡掉的请求同样计数，且排在鉴权之前——否则「某租户一直拿错 key 打网关」在指标里是隐身的。SCG 自带的 `http_client_requests_seconds` 只有 route 维度，没有租户与厂商，这是自建一条指标的理由。

上游流中途卡住由 `ai-gateway.upstream.idle-timeout` 兜（默认 60s）：路由 metadata 的 `response-timeout` 只管「等响应头」，正文阶段没有超时，见 `docs/requirements.md` 决策 D2。

## 限流

两个维度，配额都在 `ai-gateway.rate-limit.*`：

| 维度 | 桶 | 键 | 挡什么 |
|---|---|---|---|
| 租户 | `RequestRateLimiter` + Redis 令牌桶 | `tenant:<租户>` | 单个调用方打爆网关 |
| 厂商 | 自研 `ProviderQuotaFilter`，Redis 固定窗口 | `provider:<厂商>` | 所有租户合计把上游账户配额打满 |

被限一律 429 + `Retry-After`，请求不会碰到厂商。厂商维度为什么不用第二个 `RequestRateLimiter`、窗口为什么是 60 秒而不是 1 秒，都写在 `docs/requirements.md` 决策 D5。

```
curl -si http://127.0.0.1:8088/deepseek/v1/chat/completions \
  -H "Authorization: Bearer sk-team-a-local-dev" -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}' | head -5
```

```
HTTP/1.1 429 Too Many Requests
Retry-After: 59
X-Provider-RateLimit-Limit: 1
{"error":{"message":"Provider 'deepseek' quota is exhausted, retry shortly.","type":"rate_limit_error","param":null,"code":"provider_rate_limit_exceeded"}}
```

## 模型路由

选厂商有两条路，各条路由都带同样的熔断与改道：

- 不点名：一个 `/v1/chat/completions` 入口，按请求体里的 `model` 选厂商。`Model` 是自定义谓词，
  配置里写 `- Model=deepseek*`；专名放前面、兜底放后面，因为 SCG 取第一条匹配的路由，
  没匹配上任何专名的落到 dashscope。
- 点名：改路径前缀，两家对称——`/dashscope/v1/chat/completions` 与 `/deepseek/v1/chat/completions`，
  OpenAI SDK 那边就是换 `base_url`，不看 `model` 字段。

同一个模型还能用 `Weight` 按权重切给两条路由做灰度。

```
curl -s http://127.0.0.1:8088/v1/chat/completions \
  -H "Authorization: Bearer sk-team-a-local-dev" -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"用一个词回答：海的颜色"}],"max_tokens":128}'
```

```
{"id":"62fa081b-...","object":"chat.completion",...,"model":"deepseek-flash","choices":[{"index":0,"message":...
```

同一个请求把 `model` 换成 `qwen3.7-plus`，回来的就是 `"model":"qwen3.7-plus"`——厂商被名字换了，
请求体一个字节没动。为什么不用内置的 `ReadBody`、虚拟模型名为什么没做，见 `docs/requirements.md` 决策 D7。

## 熔断与改道

两家互为备份：`/v1/chat/completions`（DashScope）失败时 `forward` 到内部备份路由
`/backup/deepseek/v1/chat/completions`，反之亦然。`CircuitBreaker` 必须排在
`SetPath` / `SetRequestHeader` 之前，`resilience4j.timelimiter` 的默认 1 秒必须改大——两条都是实测出来的，
理由写在 `docs/requirements.md` 决策 D6。

只有 connect 级失败客户端能感知到改道；5xx、请求写出后被 reset、流到一半断这三种，熔断会计数、
备用会被敲，但客户端拿到的仍是主厂商的结果。

## 测试

```
mvn -pl gateway test          # 约 100 秒，熔断那类要真连死端口
```

36 个用例走进程内桩与本机 Redis：10 个覆盖鉴权、租户与厂商隔离、两条厂商路由各自的凭证与路径、报文原样、流式逐块、超时语义与指标维度；2 个覆盖空闲看门狗；6 个覆盖限流键与 `Retry-After`；5 个走 Redis 里的真实桶（租户桶、厂商共用桶、状态确实在库里），本机没起 Redis 就整类 skip；8 个覆盖熔断与改道（含「慢不等于死」那条 TimeLimiter 回归）；5 个覆盖按 model 分流、谓词读完正文后请求体仍完整、5:5 灰度与未知模型 404。真机冒烟在 `ProviderLiveTest`，按每家的环境变量单独开关，缺哪个 skip 哪个：

```
mvn -pl gateway test -Dtest=ProviderLiveTest
```
