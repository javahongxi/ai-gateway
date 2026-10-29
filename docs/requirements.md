# AI 网关需求

基于 Spring Cloud Gateway 的大模型网关：对外提供 OpenAI 兼容接口，直连多家模型厂商，负责路由、鉴权、限流、熔断与观测。

## 一、边界

网关只负责把一次模型调用送到该去的厂商，并把响应原样流回来。四件事不在范围内：

- 计费与结算。网关**采集**用量（R12：prompt / completion / cached / reasoning tokens，进指标与请求日志），但不**结算**——不落库、不扣费、不管倍率、不做充值与出账。这一条原先写成"计费与配额结算"一整块，把采集也顺带划走了，跟 R8 那句"生产上想按 token 量计价式地扣，就把它调大"自相矛盾：按量扣的前提是先有量。故拆成"计量在内、结算在外"。
- 语义缓存与对话缓存。需要向量检索，属另一层。
- 内容安全与敏感词。需要策略库与合规口径。
- Prompt 编排、Agent、工具调用。属应用层。

## 二、需求

验收标准是这一层的核心：每条需求都写成能被一次请求证伪的形式。

| # | 需求 | 验收标准 | Spring Cloud Gateway 5.0.2 落点 | 状态 |
|---|---|---|---|---|
| R1 | OpenAI 兼容入口 | 客户端只改 `base-url` 与 `model` 就能打下游多家；`/v1/chat/completions`、`/v1/embeddings`、`/v1/models` 三条路径可用 | `Path` + `Method` 谓词，`SetPath` / `RewritePath` | 内置够用 |
| R2 | 厂商适配 | 一个请求只落一个厂商端点，网关不改写请求体与响应体 | 路由选择上游 host，`SetRequestHeader` 换 `Authorization`，`SetPath` 归一路径 | 见决策 D1；自 D9 起请求体只允许改 `model` 一个字段，其余照旧（R19） |
| R3 | 模型路由与灰度 | 按请求体里的 `model` 选厂商；同一模型可按权重切流量 | 自研 `Model` 谓词 + 内置 `Weight` 谓词 | 已实现，见决策 D7 |
| R4 | 流式与超时 | 上游不回响应头时按预算返回 504；持续吐字的长回答不被总时长切断 | `routes[n].metadata.response-timeout`（毫秒） | 见决策 D2 |
| R5 | 失败重试 | 只对尚未开始下发的请求重试；重试不重复计费 | `Retry` 过滤器 | 见决策 D3 |
| R6 | 熔断与降级 | 厂商错误达阈值后短路，切备用厂商或返回明确降级 | `CircuitBreaker` 过滤器 + `fallbackUri: forward:...` 引到备用路由 | 已实现，见决策 D6 |
| R7 | 鉴权与租户隔离 | 无 key、错 key、已吊销 key 一律 401；租户身份贯穿日志、指标、限流键 | 自研 `GlobalFilter`，排在路由之前 | 自研 |
| R8 | 限流 | 按租户与按厂商两条维度独立限；被限返回 429 带 `Retry-After`，且请求不碰到厂商 | 租户走 `RequestRateLimiter` + 自定义 `KeyResolver`；厂商走自研 `ProviderQuotaFilter` | 已实现，见决策 D5 |
| R9 | 可观测 | 每个请求产出 `tenant / provider / route / 状态 / 耗时` 指标 + 首字延迟；traceId 传到厂商；运维面不与业务端口共用 | 自建 `ai.gateway.requests` 与 `ai.gateway.ttft` Timer + micrometer-tracing/OTel + actuator 单独端口 | 已实现；日志 MDC 是已知缺口，见决策 D8 |
| R10 | 配置动态下发 | 改 Nacos 配置后路由、权重、密钥即时生效，不重启不断流 | `RouteRefreshListener` 监听 `RefreshScopeRefreshedEvent` | 已实测，见决策 D4 |
| R11 | 流中途空闲保护 | 厂商开始流式之后停止吐字，网关在空闲预算内断开，不挂死连接 | `HttpClientCustomizer` 给上游通道加 `ReadTimeoutHandler` | 已实现，见决策 D2 |

### 本批新增（对标 new-api，全部未做）

对标的是 new-api 的**机制**，不是它的范围。支付、卡密、订阅、用户体系、casbin 权限、前端控制台那一整块属"分发平台"，不在本表内；理由与出处见决策后的「对标出处」。

| # | 需求 | 验收标准 | Spring Cloud Gateway 落点 | 状态 |
|---|---|---|---|---|
| R12 | 用量计量 | 同一次对话，流式与非流式拿到同一组 `prompt / completion / cached / reasoning` 数；被拒（401/403/429）与首字节前失败的请求不产生 usage；厂商没回 usage 时记成 `unknown` 而不是 0 | 只读响应装饰器（`order -3`，早于 `NettyWriteResponseFilter` 的 -1）逐块 peek，取第一个带 `completion_tokens` 的块 | 已实现，见决策 D11 |
| R13 | 请求级结构化日志 | 每个请求产出一行 JSON，含 `tenant / token / route / provider / requested_model / upstream_model / status / 耗时 / ttft / usage / traceId / 上游 request id`；同一 `traceId` 能在日志与 trace 之间对上 | `AccessLogFilter` 一行 JSON；`traceId` 那格只印客户端带来的 `traceparent`（MDC 那半条仍是 D8 的缺口） | 已实现，见决策 D11 |
| R14 | 语义失败识别 | 厂商回 **200 但正文里是错误**（限流、欠费、内容审核拒绝）时，判定表命中并计入熔断一次；连续命中达到阈值后改道备用厂商；关键词与状态码映射表可经 Nacos 热更 | 响应侧只读判定（`ResponseObservationFilter`）；命中后由本类数连续次数并直接 `transitionToOpenState()` | 已实现，见决策 D12 |
| R15 | 厂商探活与自动恢复 | 熔断打开后，低频探测（`/v1/models`，不调真模型）成功即自动收拢；探测请求不进入业务指标分位数，也不产生 usage | `@Scheduled` + `WebClient`，写熔断状态与一个 gauge | 未做 |
| R16 | 密钥池 | 同一厂商两把 key 时，厂商维度窗口键按 `provider:key` 分桶——一把 key 被上游限流不影响另一把的配额；日志记用的是哪把（脱敏后 4 位） | provider 声明里的 `keys[]` + `ProviderQuotaFilter` 的键拼接 | 未做，与 D10 同批 |
| R17 | 令牌维度治理 | 令牌打未授权模型 → 403 且不碰厂商；来源 IP 不在 CIDR 内 → 403；令牌过期 → 401。三条都在限流之前、在碰到厂商之前 | `AiGatewayProperties.tenants` 加 `allowed-models` / `allow-ips` / `expires-at`，判定在 `TenantAuthFilter` | 未做 |
| R18 | 上游余额与配额探活 | 定时拉厂商账户接口，产出 `ai_gateway_provider_balance` gauge；拉取失败只打点与一条 warn，**不改变任何路由行为** | `@Scheduled` + `WebClient`，与 R15 共用一套探测框架 | 未做 |
| R19 | 请求侧模型名改写 | 客户端传 `deepseek-v4-flash` 打到 dashscope 路由时，桩收到的是该厂商认的名字；改道到备用厂商后，第二条路由按备用厂商自己的映射再改写一次 | 请求侧过滤器 `ModelMapping`，跟随命中路由点名的 provider（args 没写时读路由 metadata 的 `provider`） | 已实现，见决策 D9 |
| R20 | 虚拟模型名与升档重试 | 一个对外名字（如 `chat-default`）后面挂多家，按优先级选主、失败后重试换档；没人认领的名字仍是 404 而不是随便塞给某家 | provider 声明 + `Model` 谓词 + R19 的改写 | 未做，依赖 D9/D10 |
| R21 | 跨协议入口（Claude Messages / Gemini 原生格式进） | 同一份对话，三种入口进、同一家厂商出；流式首字延迟不比现在差（用 `ai.gateway.ttft` 对照，不许恶化） | 转换器注册表 + 请求侧改写；响应侧要逐块重组 | 未做，排在 R12–R20 之后，理由见 D9 末段 |

### 对标出处

上面这些行的实现参照来自本机克隆的 `new-api`（路径相对 `/Users/hongxi/github/new-api`，本次逐个点开过存在性与关键符号）。列在这里是为了**将来能回源核对**，不是引经据典：

- 路由与优先级：`model/ability.go`（`group × model → channel` 的纯决策索引表，不含凭据）、`model/channel_cache.go:117` `GetRandomSatisfiedChannel`（按 priority 分档，第 n 次重试取第 n 高档，档内权重随机）、`service/channel_select.go`（逐组耗尽优先级再切组）。→ 本表 R20、决策 D10。
- 请求侧改写：`relay/helper/model_mapped.go`（模型映射），被 `relay/compatible_handler.go` 在选完渠道之后调用。→ R19、决策 D9。
- 语义失败与容灾：`service/channel.go`（`ShouldDisableChannel`：状态码映射表 + 关键词 AC 自动机决定 auto_ban）、`controller/channel-test.go`（测活成功 `ShouldEnableChannel` 自动启用）、`controller/system_task_handlers.go`（定时测活走 system task）、`service/channel_affinity.go`（渠道亲和）。→ R14、R15。
- 用量与计费：`relay/helper/stream_scanner.go:120`（逐行扫 SSE）、`relay/helper/price.go:112`（`cacheRatio`，缓存命中单独计价）、`service/text_quota.go`（预扣与事后结算）。→ R12。我们只要它前三步的形状，不要结算。
- 令牌治理：`model/token.go:25-27`（`ModelLimitsEnabled` / `ModelLimits` / `AllowIps`）、`middleware/auth.go`（`x-api-key`、query key、IP 校验的落点）。→ R17。
- 多 key：`model/channel.go:64` `ChannelInfo` 里带 key 池与轮转。→ R16。
- 上游余额：`controller/channel-billing.go`。→ R18。
- 跨协议互转：`relay/channel/adapter.go:18` 一个 `Adaptor` 接口（`Convert{OpenAI,Claude,Gemini,Responses,…}Request` + `DoResponse`）、工厂在 `relay/relay_adaptor.go:50`；通用转换抽成了**独立模块** `relaykit/`，里面是 `relayconvert/{request,response}_registry.go` 注册表 + `internal/{oai_chat,claude_messages,gemini_chat,oai_responses}/` 双向转换器 + golden 测试。→ R21。这份组织方式（注册表 + golden，而不是 switch）是它最值得抄的一点。
- 日志表：`model/log.go:57`（user/token/model/quota/tokens/channel/group/ip/request_id/upstream_request_id）。→ R13 的字段清单照它取，只是我们落日志行不落库。

三块**不抄**的：异步任务型接口（Midjourney / Suno，`relay/mjproxy_handler.go` + `service/task_polling.go` + TaskAdaptor 的 `estimate/submit/complete` 三段式计费）——它要求网关持有任务状态，与本项目的"一次调用一条流"不是一回事；分发运营那一整块（`controller/topup_*.go`、`model/redemption.go`、`model/subscription.go`、`pkg/jsplugin`）；语义缓存与内容安全，它也没在网关层做。

## 三、决策

八条现在不定、以后要重写的判断。

### D1 网关不改写报文

`spring-cloud-gateway-server-webflux` 5.0.2 的 `filter/factory` 包里没有 `ModifyRequestBodyGatewayFilterFactory` 与 `ModifyResponseBodyGatewayFilterFactory`，只有 `CacheRequestBodyGatewayFilterFactory` 和 `RemoveJsonAttributesResponseBodyGatewayFilterFactory`。改写请求体要自己写 `ServerHttpRequestDecorator`，改写响应体在流式下意味着逐块解析再重组，首字延迟直接被做掉。

所以适配只做三件事：选上游 host、换 `Authorization`、归一 path。厂商私有协议（例如 DashScope 原生 `/api/v1/services/aigc/...`）不在这一层接，要接就用它的 OpenAI 兼容端点。需要往请求体注入字段时（例如 `stream_options`），要求客户端自带，网关不代劳。

**D9 之后这条要读成**：响应体仍然一律不改；请求体只允许改 `model` 一个字段。其余部分不变。

### D2 response-timeout 管的是响应头，不管流中途

`NettyRoutingFilter` 里被 `.timeout(responseTimeout, ...)` 包住的那个 `responseFlux`，来自 `httpClient.responseConnection((res, connection) -> ...)`——它在**上游响应头到达**时发出一个元素，之后就不再参与正文。所以：

- 上游迟迟不回响应头：超时命中，`TimeoutException` 经 `onErrorMap` 变成 `ResponseStatusException(GATEWAY_TIMEOUT)`，客户端收到 504。
- 响应头已经到达、正文卡在半路：这个超时**管不到**。`responseFlux.then(chain.filter(exchange))` 已经往下走了，正文由 `NettyWriteResponseFilter` 直接从连接里取，中间没有任何空闲检测。一条厂商不吐字的流式连接会一直挂着。

对大模型场景，第一条是主要矛盾（推理模型的首字等待可能几十秒），预算按「发出请求到首块」估，不要按整段回答时长估。第二条是需要单独补的能力，见 R11。

per-route metadata 的两个键**只接受毫秒数字**：`getResponseTimeout` 走 `Long.parseLong(metadata.get("response-timeout").toString())`，`getHttpClient` 走 `Integer.parseInt(...)`。写成 `response-timeout: 120s` 会抛 `NumberFormatException` 被 catch 掉、静默回落到全局值；`connect-timeout: 3s` 则异常没人接，直接让请求失败。键名来自 `RouteMetadataUtils`：`response-timeout`、`connect-timeout`。

看门狗（R11）补上之后管到哪：`HttpClientCustomizer` 给上游通道加了 `ReadTimeoutHandler`，配置项是 `ai-gateway.upstream.idle-timeout`。两点要说清：

- 粒度是**全局**，不是 per-route。`HttpClientCustomizer` 作用在构造网关那唯一 HttpClient 的时刻（`HttpClientFactory#applyCustomizers`），拿不到具体路由。按厂商给不同预算要另做（per-route HttpClient，或在过滤器链里自己包正文流）。
- 客户端看到的是 `PrematureCloseException: Connection prematurely closed DURING response`——连接被掐，不是干净的结束标记，也没有可读的错误事件。看门狗保住的是网关自己的连接与线程；要不要替客户端把这次中断表达成一条 SSE 错误事件，是下一个决定，而它意味着要碰响应体，与 D1 相冲，所以留着没做。

### D3 重试默认不覆盖模型调用

`RetryGatewayFilterFactory.Config` 的默认值是 `retries=3`、`series=[SERVER_ERROR]`、`statuses=[]`、`methods=[GET]`、`exceptions=[IOException, TimeoutException]`。模型调用全是 POST，不改 `methods` 等于没配重试。

更紧的约束：响应已经开始流式下发之后重试，会把两份 token 拼给同一个客户端。重试只对「拿到响应头之前」失败的请求有意义，这条要进验收用例。

### D4 可刷新的配置段必须是 JavaBean，不能是 record

`ConfigurationPropertiesRebinder` 重绑的方式是 `Binder.bind(prefix, Bindable.ofInstance(bean))`——往**已有实例**里灌值。构造器绑定的 record 没有可灌的属性，于是重绑等于什么都没做：环境里的属性变了，bean 里还是旧的。

实测过程：`AiGatewayProperties` 最初写成 record，Nacos 改 `ai-gateway.tenants.team-a.api-key` 之后日志明确打了 `refresh keys changed: [ai-gateway.tenants.team-a.api-key]`，但网关仍然认旧 key（旧 200、新 401）。改成带 getter/setter 的 JavaBean 之后，同一次改配置在 8 秒内生效，不重启。

这也解释了为什么路由能热更而租户不能：SCG 自己的 `GatewayProperties` 就是 JavaBean。推论：凡是要靠 Nacos 刷新生效的配置段，一律 JavaBean；`@RefreshScope` 不是出路，record 是 final 类，CGLIB 代理不了。

### D5 厂商维度的限流只能自己写

先按直觉做过：一条路由上挂两个 `RequestRateLimiter`，一个按租户一个按厂商，各配各的 `redis-rate-limiter.*`。跑出来两条维度用的是同一份配额。

原因在 `AbstractRateLimiter`：它 `onApplicationEvent(FilterArgsEvent)` 时**不区分这个事件是给谁的**——每个 `RateLimiter` bean 都会收到每条路由上每个 RequestRateLimiter 过滤器的 args，然后按 routeId 写进自己那份 config map。于是同一条路由的第二个过滤器把第一个的配额覆盖掉，两个 bean 都变成 100/100（实测把两个 bean 的 `toString()` 打出来看得清清楚楚）。SCG 5.0.2 里也没有能改 `CONFIGURATION_PROPERTY_NAME` 的构造器，子类化换 property name 这条路也不通。

所以厂商维度改成自研 `ProviderQuotaFilter`：一次 Redis `INCR` + 首次 `EXPIRE`，固定窗口计数，键是 `ai-gateway:provider:{provider}:{窗口序号}`。三点取舍：

- **固定窗口不是令牌桶**。窗口边沿最坏放过两倍量，但这一维要挡的是「持续打爆上游」，不是精确速率；换成令牌桶就得把 `RedisRateLimiter` 那段 Lua 搬过来。
- **窗口默认 60 秒，不是 1 秒**。这是实测出来的必要性：一次模型调用本身要几秒到几十秒，把窗口设成 1 秒、配额设成 1 连打两次，两次都 200——顺序请求永远落在不同窗口里，限了等于没限。上游账户配额一般也是按分钟给的。
- **Redis 不可达时放行**。把配额存储的可用性乘进网关的可用性不值当，这一维本来就是软保护。租户维度交给 `RequestRateLimiter`，它挂了是拒绝——两者不一致是有意的。

`Retry-After` 也由这一维自己算（还要等几秒窗口翻篇），`RetryAfterFilter` 只在没人给过的时候兜 1 秒。

顺带一条与 SCG 内置实现的差异：`RedisRateLimiter` 的键是 `request_rate_limiter.{routeId.id}.tokens`，带 routeId；所以同一个 `KeyResolver` 在不同路由上本来就是不同的桶。之前担心的「租户名与厂商名撞桶」在租户/厂商两维之间不会发生（它们在不同路由上也就算了，同一路由上 key 前缀 `tenant:` / `provider:` 仍然保留，是给人看的清楚）。

### D6 熔断改道能走通，但只有「连不上」这一种客户端看得见

`CircuitBreaker` 配 `fallbackUri` 之后的行为（读源码 + 桩实测）：失败时清掉响应状态、把
`GATEWAY_REQUEST_URL_ATTR` 换成 fallback 路径、`ServerWebExchangeUtils.reset(exchange)` 释放上游连接，
然后重新走一遍 `DispatcherHandler`。所以 fallback 能被**另一条路由**接住——这就是改道到备用厂商的通路，
两条真实厂商互为备份的拓扑在 `application.yml` 里。它还会 `enableBodyCaching(routeId)`，请求体因此可重读，
文档里那条「`ReadBody` 能不能重读」的未测卡点就是这个（原先只由这段代码回答，现在由
`CircuitBreakerFailoverTest#deadPrimaryIsServedByTheBackup` 里那句 body 全等断言钉住）。

四件事必须记下来：

1. **`CircuitBreaker` 要排在 `SetPath` / `SetRequestHeader` 之前。** 它改道时用的是「自己看到的那个 request」；厂商 key 若已被前面的过滤器换进去，重投递时鉴权读到的是厂商 key，直接 401。这条是第一版实测撞出来的。
2. **`resilience4j.timelimiter` 的默认 `timeout-duration` 是 1 秒。** spring-cloud-circuitbreaker 会把每次调用包在 TimeLimiter 里，大模型没有 1 秒回来的调用，于是**每个请求都被判超时走 fallback**。真机上表现为：qwen 请求正常返回，客户端却拿到 DeepSeek 回的 400。生产配置里显式给了 120s，与路由的 `response-timeout` 对齐；`CircuitBreakerFailoverTest#slowPrimaryIsNotMistakenForADeadOne` 钉住这条。
3. **只有 connect 级失败能被客户端感知到改道。** 端口没人听 → 客户端拿到备用的 200。而「连上了、请求写出去了、然后 5xx / 被 reset / 流到一半断」这三类，熔断器会计数、备用厂商也会被敲，但客户端拿到的还是主厂商那个失败——响应已经开始走，替换不了。这未必是坏事：请求已经进了厂商，改道可能把同一次计费算两遍。

   测这三类的桩修过两处，都值得记着：`RESET` 原先写的是 `Mono.error(new IOException(...))`，reactor-netty 会替它回一个 HTTP 500，于是那条叫「连接被 reset」的用例其实测的是 500，与隔壁 `FAIL_500` 重复（日志里 `Error starting response. Replying error status` 就是证据），现在改成不写任何响应、直接关 socket；改完的真实机制是 `PrematureCloseException: Connection prematurely closed BEFORE response` 被 `AbstractErrorWebExceptionHandler` 落成 500，说明传输层错误是从写响应那一侧冒出来的，绕过了熔断过滤器的作用域。`SSE_THEN_BREAK` 原先在吐一块之后立刻 `concatWith(Mono.error(...))`，错误可能赶在第一块 flush 之前把连接关掉，用例就在「收到一块」与「什么都没收到」之间飘，现在中间留 300ms。
4. **跨厂商改道带着一个模型名问题。** 请求体里的 `model` 是原厂商的名字，改道后新厂商多半不认识——真机实测：`qwen3.7-plus` 被改道到 DeepSeek，回来的是 `The supported API model names are deepseek-flash, deepseek-v4-pro`。要按厂商改写 `model` 就得动报文，与 D1 相冲；这件事和 R3 的「虚拟模型名 → 各厂商真实模型名」是同一个问题，留到四期一起解。

还有一条环境性的：这台机器上「连一个没人听的端口」经网关要耗约 10 秒才失败（`curl` 直连是 0.2 毫秒），所以熔断用例的滑动窗口设成 2、真连死端口的用例只留两条，否则一个测试类要跑两分钟。

### D7 模型路由自己实现一个谓词

内置的 `ReadBody` 用不了多深：它的 `predicate` 参数类型是 `java.util.Predicate`，SCG 里没有
String→Predicate 的转换器，配置里只能写 `#{...}` 去引用一个 Java 侧 bean——绕一圈还是得写 Java，
却把匹配逻辑塞进一个 SpEL 字符串。而「按模型分流」是大模型网关的核心概念，值得有自己的谓词名。
于是有了 `ModelRoutePredicateFactory`：配置写起来就是 `- Model=deepseek*`。

读请求体的方式与 `ReadBody` 相同（`cacheRequestBodyAndRequest` + `ServerRequest.bodyToMono`），
两件事因此被实测确认：谓词读过之后，转发给厂商的请求体**逐字节不变**
（`ModelRoutingTest#theBodyStillReachesTheProviderByteForByteAfterThePredicateReadsIt`）；
多条候选路由共用同一份缓存，第二次命中不会去重读一个已被消费的流
（`#weightAndABodyReadingPredicateCoexistOnTheSameGroup`，文档里最后一条未测项就此结掉）。
匹配用 `PatternMatchUtils` 的 `*` 通配而不是正则——模型名里全是点号、冒号和连字符。

三条配套结论：

- **路由顺序就是优先级。** SCG 取第一条匹配的路由，所以 `Model=deepseek*` 这类专名要排在兜底前面。
- **虚拟模型名（一个名字后面挂多家）没做。** 它要求把 `model` 改写成各厂商的真实名字，也就是动报文，
  与 D1 相冲。现在的做法是「客户端传什么名字就按名字选厂商」，两家都能服务的名字（如 `deepseek-*`）
  才有跨厂商改道的意义。真要做，改写只发生在**请求**侧、且是几 KB 的一次性缓冲，不影响流式首字延迟，
  代价是 D1 要开一个明确的例外——留到真有这个需求时再开。
- **`default-filters` 清不掉。** `spring.cloud.gateway.server.webflux.default-filters=`（空值）不会
  覆盖 yml 里那份列表——Boot 的集合绑定按索引逐个属性合并，空值不构成一次覆盖。实测表现：某个测试类
  明明写了这句，连打 40 次之后第 34 次还是吃了 429。不测限流的类因此统一 `@Import` 一个
  `AlwaysAllowRateLimiter`（`@Primary` 的 `RateLimiter`），而不是假装能清空配置。

### D8 traceId 传播是通的，日志 MDC 不是

`spring-boot-starter-opentelemetry` 加上之后，实测三件事：

- **厂商收得到 `traceparent`。** SCG 的 `ObservedRequestHttpHeadersFilter` 在发请求前把 W3C 头注进去，桩侧断言格式为 `00-<32hex>-<16hex>-<flags>`。这才是排障真正需要的东西：厂商侧日志能用同一个 id 对上。
- **`management.tracing.export.enabled=false` 不是「只关导出」。** 设成 false 之后 tracer 不再产生，连 `traceparent` 都不注了——传播和导出是一套开关。所以生产配置保持 true。
- **指标的 OTLP 推送要单独关。** `management.otlp.metrics.export.enabled: false`。先前我在文档里写「没有 collector 时导出静默失败、没有噪音」，那是错的：只 grep 了一次很短的输出就下了结论。实际是 `OtlpHttpMetricsSender` 每 30 秒往 `http://localhost:4318/v1/metrics` 推一次，一次全量测试里刷出 48 行 `ConnectException`。指标本来就已经由 8089 上的 `/actuator/prometheus` 被拉走，推 OTLP 是第二条通路，本地没有 collector 就是纯噪音。关掉之后同一次全量测试里 `ConnectException` 归零；traces 侧在这次跑里没有导出失败的记录。
- **WebFlux + OTel bridge 下 MDC 里没有 traceId。** 请求执行中抓 `MDC.get("traceId")` 是 null，`Tracer.currentTraceContext()` 与 `ObservationRegistry.getCurrentObservation()` 也都是 null——reactive 没有线程绑定的 scope。所以日志格式里写 `%X{traceId}` 只会打出空括号，这句已从 `application.yml` 去掉。要修：加 OTel 的 logback-mdc instrumentation，或者从 Reactor Context 里取 span 自己写日志。这条以「已知缺口」的形式钉在 `ObservabilityTest#traceIdIsNotInMdcUnderTheOtelBridge`，修好了把断言反过来写，别删。

### 首字延迟挂在响应装饰器上

`ai.gateway.ttft` 记的是「进入网关 → 第一个字节交给客户端」。挂点是 `NettyWriteResponseFilter`：它走 `exchange.getResponse().writeAndFlushWith(...)`（流式）或 `writeWith(...)`，所以只要排在它之前（order `-2`）把 response 换成 `ServerHttpResponseDecorator`，就能在字节真正交给客户端那一刻记时。这是**客户端看到的**首字节，不是厂商返回的首字节。

只观察不改字节，与 D1 不冲突。两个配套决定：非 2xx 的响应不记样本（401/429/504 写出去的是错误正文，混进分位数会把指标拖歪）；一个字节都没交出去时也不记。

真机对照（一次 `max_tokens=200` 的流式回答）：`ai_gateway_ttft_seconds_sum` 1.454 秒，`ai_gateway_requests_seconds_sum` 2.057 秒。只看后者会把「多久开始吐字」和「一共吐了多久」混成一个数。

### D9 网关不改报文这件事开一个口子：响应侧永远只读，请求侧允许改写 model

`状态：已实现（R19）。三条待验都跑过实测，其中一条是被 20 秒挂死逼出来的。`

- **改道时二次生效**：备用路由自己的改写会生效，且它映射的键是客户端**原始**那个名字（存在属性里，不是已被主厂商换成真名的那个）。`GeneratedRoutesAndModelMappingTest#failoverReMapsAgainstTheBackupProviderFromTheOriginalName` 钉这条。反证也做了：把备份路由的改写摘掉，该用例确实红，而且红得有意义——备用厂商收到的是原始的 `chat-default`（主厂商连不上，它那一侧的改写根本没落地）。所以改道链路上真正干活的是备用路由自己那一步，不是主路由的预测。
- **`Content-Length` 由本过滤器重算，不靠 SCG**：桩侧加了 `contentLength()` 取值器，两条用例断言它等于实际字节数。不重算的话厂商要么截断要么一直等正文——第一次跑就是等满 20 秒被客户端超时抓出来的。
- **一名对两名的定性**：表里没有的名字原样透传（`#modelAbsentFromTheTableIsForwardedByteForByte`，逐字节相等），所以灰度组里两条路由映射不同不构成歧义——每条路由只回答"我这个厂商叫什么"。`requested_model` 与改写后的名字分别留在 exchange 属性与 `ModelMappingGatewayFilterFactory.requestedModel()` 里，等 R13 的日志来取。
- 一个必须记下的机制：`ServerWebExchangeUtils.cacheRequestBody` 在 filter 这一侧**只把字节存进属性、不换 request 对象**（只有谓词用的 `cacheRequestBodyAndRequest` 会缓存 decorator，见 `ServerWebExchangeUtils:360-411`）。所以"读过但决定不改写"也必须交回一个可重放的体，否则下游 `NettyRoutingFilter` 拿到的是被抽干的流——那次 20 秒挂死的根因就是这个。

D1 原先一刀切"不改写报文"，代价是文档里三个卡点都解不开：D6.4 那次真机失败（`qwen3.7-plus` 改道到 DeepSeek，回来一句 `The supported API model names are deepseek-flash, deepseek-v4-pro`）、D7 里"虚拟模型名没做，因为要动报文"、以及 R20 整条。三条是同一个原因，所以现在把口子开到刚好够：

- **响应侧永远只读。** 这是 TTFT 和 R12 能成立的前提——一旦重组响应体，流式首字就被做掉了。
- **请求侧只改 `model` 一个字段。** 不注入 `stream_options`，不动 messages，不改采样参数。D1 原来那句"需要往请求体注入字段时要求客户端自带"保留——那一类改动是把厂商差异推给业务代码，而 `model` 名映射恰恰是网关存在的理由。
- 代价说清：一次改写是一次几 KB 的对象重建，发生在请求侧、请求还没开始流，所以不影响首字延迟。这条论证 D7 末段已经写过，不是新论据。

真正的坑是**顺序与归属**，而且它和 D6 那条顺序约束方向相反：

1. 改写必须跟着**最终命中的路由**走。熔断改道会重走一遍 `DispatcherHandler`（D6 已证），所以备用路由自己的改写过滤器会自然生效——不要去主路由上"预测改道后会落到哪家"，那是把拓扑知识塞进过滤器。
2. 因此过滤器排在 `SetRequestHeader` 附近、读 `metadata.provider`，且**排在 `CircuitBreaker` 之后**：熔断要看原始请求（D6.1），改写要看最终请求。yml 里的过滤器顺序又要人记一遍——这正是 D10 要消灭的东西。
3. 映射必须**幂等**：目标名字找不到映射时原样透传，不抛错。否则改道后二次生效会把已映射的名字再映射一次，表现是"第二次重试报 unknown model"，比原来那个 400 更难查。
4. 指标与日志从此有两个维度：`requested_model`（客户端传的，路由按它选）和 `upstream_model`（改写后上线的）。只记后者会让"客户端传的模型没人认领"这类 404 排障对不上，R13 的字段就是从这里来的。

做完必须回填实测、现在还是假设的三件事：①改道重投递时过滤器二次生效的实际行为（幂等假设的验证）；②`Content-Length` 在改写后由 SCG 重算还是沿用旧值（桩上看一眼就行，沿用的话请求会被厂商截断或拒收）；③Weight 灰度组里两条路由映射不同时，同一个 `requested_model` 对应两个 `upstream_model` 是预期还是配置错误——要给个说法，别留给读日志的人猜。

R21（Claude Messages / Gemini 入口）排在它后面，不是因为难，是因为它要撞第二次这个口子：跨协议必须改响应体，那是 D1 的另一个例外，得单独决策，别把两件事捆在一起做。

### D10 路由表从手写 URI 拓扑改成 provider 声明加生成

`状态：已实现。三个卡点都有结论，其中第三个是意外收获。`

- **卡点①（生成与手写混排的顺序）答案是 `RouteDefinition.order`，不是 locator 顺序**：`CachingRouteLocator:65` 用 `AnnotationAwareOrderComparator` 给路由排序，读的是每条路由自己的 order。生成的路由统一 `order=1000`、手写的默认 0，于是**手写优先、生成兜底**，`spring...routes` 成了真能用的逃生舱（`#handwrittenRouteShadowsTheGeneratedFallback`）。
- **卡点③（刷新幂等）**：生成器无状态，同一份声明 `build` 两次产出同样的列表（`ProviderRouteDefinitionLocatorTest#buildingTwiceProducesTheSameListWithoutAccumulation`）。
- **卡点②（改拓扑瞬间的在途请求）没验**：那要"边打流量边改配置"的测试，成本明显更高，留到做 R15 探活时一起补。没测的就写没测。
- **意外收获，也是本批唯一的架构级发现**：生产的 `providers` 一进 `application.yml`，就会**漏进每一个测试上下文**。声明里那个不认领模型的兜底厂商，把 `ModelRoutingTest` 的"没人认领的模型该 404"抢成了打到真实厂商。解法是开关 `ai-gateway.generated-routes`（生产 true，自带路由表的测试类 false），顺带它也是运维的急停闸。记在这里是因为这类问题一定重犯：**凡是"由声明生成配置"的东西，都要同时回答"测试怎么把它关掉"。**
- 装配层面的两个键名教训：`SetPath` 的参数键是 `template`，`SetRequestHeader` 的是 `name` / `value`（`AbstractNameValueGatewayFilterFactory.NameValueConfig`）。写错不是静默失效，而是**上下文启动即 NPE**（`template must not be null`），这类错误在 CI 里活不过第一次跑——这也是一次性把拓扑规则搬进代码的回报。

两家厂商现在已经是 6 条路由、两条顺序约束（熔断在前、专名在前）和一堆 `forward:/backup/...` 字符串。四家就是 12 条。问题不在行数，在**fallback 目标是个字符串**：写错的代价是一次"配置看着通、改道打到一个不存在的 route id"的静默失败，而它既不在代码里也不在测试里，只在某个周五晚上。

对标的是 new-api 把路由决策单独做成一张表这件事：`model/ability.go` 是 `group × model → channel` 的纯决策索引，里面不放凭据、不放拓扑；`channels` 表只持有上游事实。我们没有数据库，等价物就是"配置态的 ability 表"——providers 声明 + 生成器。

provider 段至少要说清这些（第四节草稿里已有前三个，其余补上）：`base-url`、`key-ref` 或 `keys[]`（R16）、`path`、`response-timeout` / `connect-timeout`、`model-mapping`（D9 的数据来源）、`backup`（替代手写的 fallbackUri）、`provider-window`（R8 厂商维度）、`health-path`（R15、R18 的探测端点）。生成器的产出就是把 `backup` 拼成 `forward:/backup/{id}/...`。

值得多写这层代码的唯一理由是：**那三条靠人记的顺序约束可以进代码并被单测钉住**。顺序、备份路由不挂熔断、`/backup/` 前缀——配置里的规则是约定，代码里的规则是测试。

两条既有结论必须服从，都是踩过一次换来的：D4——providers 段必须是带 getter/setter 的 JavaBean，record 重绑等于没做；D5——厂商维度配额别试图塞进第二个 `RequestRateLimiter`，`AbstractRateLimiter` 收 `FilterArgsEvent` 时不分来源，会互相覆盖。

待验证卡点：①生成路由与手写路由混排时的匹配顺序（`RouteDefinitionLocator` 的合并次序，逃生舱要排在哪儿得先定）；②Nacos 改 providers 触发重建那一瞬间，正在改道中的在途请求会不会指向一个刚消失的 route id——R10 验过"改 key 即时生效"，没验过"改拓扑时在途请求"，这是两件事；③刷新两次不能把路由翻倍（生成器幂等）。

### D11 观测这一层各看各的字节，不做跨过滤器的属性传递

`状态：已实现（R12、R13）。这条是被两次随机红逼出来的，不是设计出来的。`

先说结论：**响应侧的每一个观察者自己装饰一次响应、自己 peek 字节**，观察者之间不传数据。`UsageRecordingFilter`（order -3）产出计数器，`AccessLogFilter`（order -2 之前，同为响应装饰器）自己再扫一遍同一份正文来填日志里的 `usage` 与 `ttftMs`。两处共用同一个 `extract()`，所以扫描规则只有一份——重复的是读字节这个动作，不是判断逻辑。

理由分两层，都是实测：

- **内层写的属性，外层读不到。** 第一版按常规写法做：用量过滤器把采到的 `Usage` 放进 exchange 属性，日志过滤器在 `doFinally` 里读。单跑那条用例是绿的，整包跑就红——而且红得毫无道理：日志行里 `status` 明明白白是 200。原因是这两个过滤器之间隔着 `mutate()`：装饰响应要 `exchange.mutate().response(...).build()`，本类拿到的是 mutate **之前**的那个对象，写进去的属性表与外层读的表不是同一份。`TenantAuthFilter` 的租户属性能被外层看到，恰恰因为它自己不 mutate。**结论：写属性要写在你会读的那个对象上；跨 mutate 的传递不算传递。**
- **日志行与客户端拿到最后一个字节之间没有 happens-before。** 那一行是在外层过滤器的 `doFinally` 里落的，客户端的响应可以早一点点完成。于是"调用返回后立刻读 appender"这种断言会随机读到空——三个用例同时红过。修法不是加 `sleep`，是**先记下已有行数，调用后等到它变多**（`awaitNewLine`，上限 5 秒）。这不是把测试写松：它断言的是"这一发确实留下一行"，比 `hasSize(1)` 更准。

两条一起指向同一个取舍：多读一遍几 KB 的正文，换掉一条会悄悄失效的跨层通道。真有大正文的量级再说，那是 `Flux` 上的一次 `scan` 而不是重构。

第三个消费者（R14 语义失败）出现时，两个观察者合并成了一个 `ResponseObservationFilter`：一次 peek、三个出口（token 计数、语义开闸、日志一行）。两遍读字节已经是要解释的账，三遍说不下去。合并重复的是**读字节**，判断逻辑仍然只有一份（`extract` 与 `detectSemanticFailure` 都是静态纯函数，可单测）。

顺手记三条实现层的，都各红过一次：

- `statusCode` 要在**写字节那一刻**记，不能在 `doFinally` 里读。读到的可能是 null，于是所有正常响应都被判成"没发生"，用量全线静默归零。
- 厂商没回 usage 时记 `absent`（`ai.gateway.usage.samples{outcome=absent}`），日志里是字符串 `unknown`，**不是四个 0**。把"没采到"印成"用了 0"是指标最典型的说谎方式，而真机确实会这样。
- 扫描规则是"逐块扫，取第一个带 `completion_tokens` 的"，不是"倒数第二个块"。桩里三个场景（非流式、DeepSeek 把 usage 挂在 finish 块、DashScope 在 `[DONE]` 前多给一个 `choices: []` 的块）走同一条规则得到同一组数，这条以前只是文档里的一句话。

### D12 语义失败的开闸方式：自己数连续命中，不往熔断窗口补记事件

`状态：已实现。`

厂商回 HTTP 200、正文里是限流/欠费/审核拒绝，这一类错误率看不见。判定命中之后有两条路能把它接进熔断：调 `CircuitBreaker#onError`，或者自己数连续次数、到阈值直接 `transitionToOpenState()`。先选的是前者，被用例打回：

熔断那台状态机已经把这次请求记成了一次 **success**（HTTP 确实 200）。再塞一个 `onError` 事件，窗口里这一发就占了**两个位置**、失败率立刻 50%——窗口 2、阈值 50 的配置下**一发**就把闸打开了，`upstream.hits` 少一次就是这么来的。同一发请求记两笔账不是"连续失败"的语义。

所以现在：`ai-gateway.semantic-failure-threshold`（默认 3）数**连续**命中，中间出现一次正常响应就清零；到阈值直接开闸，之后半开探测由熔断自己的配置负责。反证做过——阈值改成 99，用例里的闸门状态就停在 `CLOSED`，说明断言真的在测开闸而不是在测窗口默认行为。

判定面只有两条，都只看**顶层结构化字段**：

- `structural`：顶层有 `error` 对象且没有 `choices`（OpenAI 兼容错误体），与厂商无关。
- `indicator`：顶层 `code`（或 `error.code` / `error.type`）等值命中该厂商 `failure-indicators` 声明的码；大小写无关，不做前缀或包含匹配。

**不做自由文本子串搜索**：模型的回答里出现 "quota exceeded" 是常态，误判会把一次正常调用记成厂商故障并无谓开闸；误判的代价比漏判大，漏判还有错误率兜着。规则单测里专门有一条盯它（带 `choices`、正文写着 "your quota exceeded" 的成功响应必须不命中）。哪些厂商有哪些码属于部署事实，本仓库不替厂商定义——测试里那些字符串是桩造的。

## 四、配置

dataId 用 `ai-gateway.yaml`，不用 properties。SCG 的路由是嵌套列表，properties 只能写成 `...routes[0].predicates[0].args.pattern=` 这种带下标的形式，改一次权重要数下标，动态下发就失去意义。属性前缀是 5.x 的 `spring.cloud.gateway.server.webflux.*`。

三段配置，都放 Nacos：

```yaml
ai-gateway:
  providers:      # 厂商注册表：别名 → base-url / 密钥引用 / 超时预算（毫秒）
    dashscope: { base-url: https://dashscope.aliyuncs.com/compatible-mode, key-ref: ${DASHSCOPE_API_KEY}, response-timeout: 120000 }
    deepseek:  { base-url: https://api.deepseek.com,                       key-ref: ${DEEPSEEK_API_KEY},  response-timeout: 120000 }
  tenants:        # 租户 → key、可用厂商、配额
    team-a: { api-key: ${GATEWAY_TENANT_KEY}, providers: [ dashscope, deepseek ] }

spring:
  cloud:
    gateway:
      server:
        webflux:
          routes:   # 由 providers / tenants 生成，或手写；Weight 谓词承载灰度
            - id: dashscope-chat
              uri: https://dashscope.aliyuncs.com
              predicates:
                - Path=/v1/chat/completions
              metadata:
                provider: dashscope
                response-timeout: 120000   # 毫秒，见决策 D2
                connect-timeout: 3000
              filters:
                - SetPath=/compatible-mode/v1/chat/completions
                - SetRequestHeader=Authorization,Bearer ${DASHSCOPE_API_KEY}
            - id: deepseek-chat
              uri: https://api.deepseek.com
              predicates:
                - Path=/deepseek/v1/chat/completions   # 点名厂商：改 base_url 的路径前缀
              metadata:
                provider: deepseek
                response-timeout: 120000
                connect-timeout: 3000
              filters:
                - SetPath=/v1/chat/completions
                - SetRequestHeader=Authorization,Bearer ${DEEPSEEK_API_KEY}
```

自定义段用顶层 `ai-gateway` 前缀，不挤进 `spring.*`。`routes` 段走 SCG 原生绑定，`RouteRefreshListener` 已经在监听 `RefreshScopeRefreshedEvent`，Nacos 改配置触发 spring-cloud-context 刷新，路由随之重建，R10 不需要代码。

一期实现的是 `tenants` 加手写 `routes`。上面的 `providers` 段自 D10 起从"目标形状"升成**本批要落的形状**：两家厂商已经是 6 条路由加两条顺序约束，阈值到了。密钥池（同一家多把 key 轮转，R16）与模型名映射（R19）都在这个段里落地，不再另起一处配置。手写 `routes` 保留为逃生舱，但备份关系不再手写。

## 五、依赖缺口

`gateway/pom.xml` 现在有 `spring-cloud-starter-gateway-server-webflux`、`spring-cloud-starter-alibaba-nacos-config`、`spring-cloud-starter-circuitbreaker-reactor-resilience4j`（R6）、`spring-boot-starter-data-redis`（R8 的桶状态）、`spring-boot-starter-actuator` + `micrometer-registry-prometheus`（R9）、`netty-resolver-dns-native-macos`（macOS 上缺它启动就一直在打 `Unable to load io.netty.resolver.dns.macos...` 的 ERROR），外加测试依赖。按需求清单还缺：

| 依赖 | 服务的需求 | 说明 |
|---|---|---|
| `spring-cloud-starter-alibaba-nacos-discovery` + `spring-cloud-starter-loadbalancer` | 内网私有化模型 | 只有接 vLLM / Ollama 集群才需要，直连公网厂商用不上 `lb://` |
| OTel 的 logback-mdc instrumentation（artifact 名以 `opentelemetry-instrumentation-logback-mdc` 为准，引之前先确认与 Boot 4 / 现有 OTel 版本匹配的那一个） | R13、补 D8 的 MDC 缺口 | 本批唯一可能新增的依赖。不想引就用第二条路：在过滤器里从 Reactor Context 取 span 自己写日志——零依赖，代价是每次都要记得走那条链。二者选一即可，别两个都做 |

R12 到 R20 这一批**不新增其他依赖**：usage 采集与语义失败判定是响应侧只读装饰器，密钥池与令牌治理是配置与过滤器，探活与余额采集用 SCG 已经带进来的 `WebClient`。这既是取舍也是约束——一旦开始为"计量"引一个 SDK，就离结算那一层不远了，而那是本文明确划出去的边界。

先前这一节写过「单机限流可以用 `Bucket4jRateLimiter`，只要显式加 `com.bucket4j:bucket4j_jdk17-core`」——不准，划掉：`Bucket4jRateLimiter` 的装配条件是容器里已有 `AsyncProxyManager<String>` bean（`GatewayAutoConfiguration.Bucket4jConfiguration` 上写着 `@ConditionalOnBean(AsyncProxyManager.class)`），而 bucket4j 的 core 包**不提供**任何本地 async 实现，要再引一个集成件（Caffeine / Redisson 那一类）。单机想省 Redis 的话，与其多拉一个依赖，不如像决策 D5 那样自己写一次 `INCR`。

根 pom 原先挂着 `spring-ai-bom` 但没有模块消费，已删：网关走 HTTP 透传，不需要 Spring AI 的模型抽象，留着只会让人以为这条链路依赖它。

## 六、实测记录

一期代码已把这些结论钉在测试里，改坏了测试会红。

| 结论 | 怎么测出来的 |
|---|---|
| 上游响应头迟到 → 504，且按 per-route 预算触发 | `GatewayChatFlowTest#lateResponseHeadBecomesGatewayTimeout`：桩 5 秒才回头，预算 300ms |
| 响应头之后的正文卡住**不会**被 `response-timeout` 切断 | `GatewayChatFlowTest#midStreamStallIsNotCutOffByResponseTimeout`：桩吐一块就沉默，1.5 秒后流仍未结束 → 这就是 R11 |
| 流式不被网关缓冲，且总时长超过 `response-timeout` 仍能收完 | `GatewayChatFlowTest#streamIsDeliveredIncrementallyAndSurvivesPastTheResponseTimeout`：7 块 × 80ms，首块在预算内、总时长超预算 |
| 网关不改报文，只换 host / path / key，并加 `X-Tenant-Id` | `GatewayChatFlowTest#nonStreamCallIsForwardedWithTheProviderCredentialAndAnUnchangedBody`：桩收到的 body 与请求体逐字节相同 |
| 无 key、错 key、越权厂商都在碰到厂商之前被挡下 | `GatewayChatFlowTest` 的 401 / 403 用例，断言桩的请求数为 0 |
| 两条厂商路由各走自己的路径与 key，租户按 provider 隔离 | `GatewayChatFlowTest#eachProviderRouteCarriesItsOwnPathAndCredential` 与 `#tenantAllowedForOneProviderCannotUseAnotherProvidersRoute` |
| 主厂商连不上时改道备用厂商，客户端拿到备用的 200 | `CircuitBreakerFailoverTest#deadPrimaryIsServedByTheBackup`：备用厂商收到 1 次、带自己的 key 与 `X-Tenant-Id` |
| 改道重投递之后请求体逐字节不变 | 同一条用例里的 `upstream.body()` 全等断言（本批补上）。做法上多验了一步：把期望值改成 `REQUEST_BODY + "SENTINEL"` 跑一次，用例确实红并报出 expected/actual，所以这条不是空断言 |
| 熔断打开后不再敲主厂商 | `CircuitBreakerFailoverTest#breakerOpensAfterAWindowOfFailures` 与 `#openBreakerStillServesTheBackupWithoutWaitingOnThePrimary`：窗口 2、阈值 50%，打开之后那一次只打备用 |
| 慢的主厂商不该被当成死的 | `CircuitBreakerFailoverTest#slowPrimaryIsNotMistakenForADeadOne`：1.5 秒回响应头，改道次数为 0。去掉 `resilience4j.timelimiter...timeout-duration` 这条就红——默认 1 秒会把每个模型调用判超时 |
| 5xx / reset / 流中途断这三种改不了道 | `#serverErrorFromALivePrimaryIsNotReplacedByTheBackup`、`#connectionResetAfterTheRequestWasSentStillReturnsThePrimaryFailure`、`#midStreamFailureCannotBeFailedOver`：熔断计数与备用都被敲，客户端仍拿主厂商的结果 |
| 同一个入口按 `model` 选厂商 | `ModelRoutingTest#modelDecidesTheProviderBehindOneSharedPath`；真机 `/v1/chat/completions` 传 `deepseek-v4-flash` 回显 `deepseek-flash`、传 `qwen3.7-plus` 回显 `qwen3.7-plus` |
| 谓词读过请求体之后，转发出去的正文一字不变 | `ModelRoutingTest#theBodyStillReachesTheProviderByteForByteAfterThePredicateReadsIt` |
| 没人认领的模型名是 404，不是随便塞给某家 | `ModelRoutingTest#aModelNobodyClaimsIsNotFoundRatherThanSentSomewhere`：`glm-4-plus` → 404，上游请求数 0 |
| 5:5 灰度真的两边分流量，且与读正文的谓词共存 | `ModelRoutingTest#weightAndABodyReadingPredicateCoexistOnTheSameGroup`：40 次两边各 >5 次 |
| 没有 JSON 正文的请求不会卡在谓词上 | `ModelRoutingTest#requestWithoutAJsonBodyDoesNotHangOnThePredicate` |
| traceId 传到厂商 | `ObservabilityTest#traceIdReachesTheProviderAsATraceparentHeader`：桩收到 `00-<32hex>-<16hex>-<flags>`；关掉导出这条就红 |
| 首字延迟与整条请求是两个数 | `ObservabilityTest#firstTokenIsRecordedApartFromTheWholeStream`：桩 7 块 × 80ms，TTFT 样本明显小于总耗时样本 |
| 被拒的请求不贡献首字样本 | `ObservabilityTest#aRejectedRequestContributesNoFirstTokenSample` |
| providers 声明真能被 SCG 装配成路由（`Model` 谓词与 `ModelMapping` 过滤器都解析得动） | `GeneratedRoutesAndModelMappingTest` 整类 7 条；真机 `ProviderLiveTest` 跑的也是生成路由 |
| 改道后请求体还在、模型名换成备用厂商的真名、`Content-Length` 跟着重算 | `#failoverReMapsAgainstTheBackupProviderFromTheOriginalName`；反证（摘掉备份路由的改写会红）见 D9 第一条 |
| 映射表没命中时逐字节原样透传 | `#modelAbsentFromTheTableIsForwardedByteForByte` |
| 手写路由优先于生成的兜底路由（逃生舱有效） | `#handwrittenRouteShadowsTheGeneratedFallback`，机制见 D10 卡点① |
| 测试类自带路由表时必须关掉生成器，否则生产的兜底厂商会抢断言 | `ai-gateway.generated-routes=false`，五个测试类各一行，原因见 D10 |
| 读过请求体就要交回可重放的体，否则厂商侧挂死 | 第一次跑挂 20 秒暴露，机制见 D9 末条 |
| 三种 usage 形状（非流式、usage 在 finish 块、usage 在多给的 `choices:[]` 块）得到同一组数 | `UsageAndAccessLogTest#allThreeUsageShapesLogTheSameNumbers` + `UsageExtractionTest` 5 条把扫描规则单独钉住 |
| 厂商没回 usage 时是 `absent` / 日志里 `unknown`，不是 0 | `#responseWithoutUsageLogsUnknownRatherThanZeros`；被鉴权挡掉的请求同样不产生 usage 且 `upstream.requests()` 为 0（`#rejectedRequestStillLogsOneLineButCarriesNoUsage`） |
| 访问日志一行带齐 `tenant/route/provider/requestedModel/upstreamModel/status/elapsedMs/ttftMs/usage/traceparent/upstreamRequestId` | `#accessLineCarriesBothModelNamesAndTheUpstreamRequestId`；两个模型名分开是 D9 那条改写的可见性来源 |
| 200 + 错误体原样发给客户端，同时记 `semanticFailure` 与指标 | `SemanticFailureCircuitTest#semanticFailureIsPassedThroughButRecorded`；判定规则另有 8 条单测（`SemanticFailureRulesTest`） |
| 连续语义命中到阈值即开闸，之后厂商恢复也被绕开 | `#repeatedHitsOpenTheBreakerAndRouteAroundTheProvider`；反证：阈值改成 99 时闸门停在 `CLOSED` |
| 正常响应不命中，自由文本里的 "quota exceeded" 也不命中 | `SemanticFailureRulesTest#successfulResponsesAreNeverSemanticFailures`、`#choicesWinOverErrorSoPartialFailuresAreNotCounted` |
| 内层过滤器写的属性，外层 `doFinally` 读不到（跨 `mutate()`） | 现象是"整包跑红、单跑绿"，机制与取舍见决策 D11 |
| 日志行落在客户端完成之后，断言必须等它变多而不是数条数 | `awaitNewLine(before)`，见 D11 第二条 |
| 日志 MDC 里没有 traceId（已知缺口） | `ObservabilityTest#traceIdIsNotInMdcUnderTheOtelBridge`，见决策 D8 |
| 真机两家都通，SSE 原样透传、content-type 不改 | `ProviderLiveTest`，按环境变量单独开关：qwen3.7-plus 16–40 次到达、deepseek-v4-flash 7 次到达 |
| 厂商开始流式之后卡住，看门狗在空闲预算内切断 | `UpstreamIdleTimeoutTest#stalledStreamIsAbortedByIdleTimeout`：看门狗 400ms、响应头预算 5s，切断落在 350–2500ms 之间。与上一条是一对——同一个场景在关掉看门狗时 1.5 秒仍未结束 |
| 看门狗不误伤持续吐字的长流 | `UpstreamIdleTimeoutTest#steadyStreamIsNotAffectedByIdleTimeout`：总时长 560ms 超过 400ms 预算，但间隔只有 80ms，完整收完 |
| 指标带 tenant / provider / route / status 四个维度，被拒请求也计数 | `GatewayChatFlowTest#everyCallIsCountedPerTenantAndProviderIncludingRejections`；真机 `/actuator/prometheus` 上能看到 `ai_gateway_requests_seconds_count{provider="dashscope",route="dashscope-chat",status="200",tenant="team-a"}`，401 记成 `tenant="anonymous"` |
| 改 Nacos 配置不重启即生效（R10） | 手工端到端：往 Nacos 发布 `ai-gateway.yaml` 覆盖租户 key，8 秒内新 key 从 401 变 200、旧 key 反向；record 版配置段做不到这件事，原因见决策 D4 |
| 租户桶超配额 429，且被限的请求不碰到厂商 | `RedisRateLimitTest#tenantBucketDeniesOnceItsBurstIsGone`：桶 2、每次扣 2，第二次就拒，桩的请求数为 1 |
| 租户之间各有自己的桶 | `RedisRateLimitTest#anotherTenantKeepsItsOwnBucket`：alpha 打满之后 beta 仍 200 |
| 厂商桶是所有租户共用的 | `RedisProviderRateLimitTest#providerBucketIsSharedBetweenTenants`：alpha、beta 各一次就把窗口用掉，第三个请求（还是 alpha）429，桩只收到 2 次 |
| 配额状态在 Redis 里，不在 JVM 里 | `RedisRateLimitTest#bucketStateLivesInRedisNotInTheJvm`：调用后能按 `request_rate_limiter.*tenant:alpha*` 扫到键 |
| 真机上厂商窗口真的会拒 | 手工：`--ai-gateway.rate-limit.provider-requests-per-window=1` 起网关，连打两次 `/deepseek/v1/chat/completions`，第二次 `429` + `Retry-After: 59` + `X-Provider-RateLimit-Limit: 1` |

运维面与业务面分端口是实测过的：`management.server.port: 8089` 下 `/actuator/prometheus` 返回 200，而 8088 上同一路径返回 404。SCG 自带的指标在 Boot 4 / SCG 5 这套组合里叫 `http_client_requests_seconds`（tag 是 `spring_cloud_gateway_route_id` 等），没有租户维度——这是自建 `ai.gateway.requests` 的理由。

`stream_options.include_usage` 两家都支持，但形状不一样：DashScope 在 `[DONE]` 之前**多给一个** `choices:[]` 的块带 `usage`；DeepSeek 把 `usage` 挂在 `finish_reason=stop` 那个块上。所以计量层不能假设「倒数第二个块」，要扫每个块取第一个带 `completion_tokens` 的。

顺带两条测法上的教训，都写进了测试注释：

- **到达次数不能当断言。** 同一次跑里 qwen 分 40 次到达、deepseek 只有 7 次——厂商一次只回一个 token 时整条流就在两三个 TCP 读里走完。「逐块不缓冲」这条性质要用固定 80ms 间隔的桩来钉，真机只验接得上、格式对。
- **usage 是否带取决于厂商，真机不断言。** 同一份请求直连 DashScope 三次都带 usage，走网关的某一次却没带（5 次里 1 次，之后两次重跑又都带）。冒烟测试断言它等于把厂商的抖动引进 CI，所以这里只打印；扫描规则由上面的形状结论承载，等计量层落地时用桩钉。
- **限流测试不能让真实时间参与。** 第一版写「桶 2、每秒补 1、第三次被拒」，三次调用本身要跑 0.5~1.8 秒，跨过秒界就补回一枚令牌，第三次反而放行——同一份代码连跑三次能给出两种结果。改成「桶 2、每次扣 2」之后，第二次必然要等两秒才可能放行，而两个本地调用之间只隔几十毫秒，断言就稳了。厂商那一维再加一个固定 `Clock` 注入，窗口键完全钉住。

到这里，二、三期列出的未测项都结掉了：`fallbackUri` 与请求体重读见 D6，`Weight` 与读正文谓词的
共存见 D7。剩下没做的都是新功能（traceId 与 TTFT、虚拟模型名、语义缓存那一层），不是没验的假设。

其中"请求体重读"那半条要说精确点：它**原先**只是读源码结掉的（熔断过滤器改道前会
`enableBodyCaching(routeId)`，出处见 D6 那段），断言没写——`deadPrimaryIsServedByTheBackup` 当时只断了
改道次数、备用厂商的 key 与租户头。现在补上了：那条用例多一句 `upstream.body()` 与请求体的逐字节比对，
并且用哨兵值验过它真的会红（改成 `REQUEST_BODY + "SENTINEL"` 之后用例失败、报出 expected/actual 两行，
说明断言在测东西而不是在装饰）。桩侧的取值器本来就有（`StubUpstream#body()`），所以这是一行成本的修补。

这件事值得单独记一笔，是因为它划出一条本项目的验收线：**源码读出来的机制不算结掉，断言才算**。
补这条不是为了好看——D9 的请求侧改写就落在同一条改道链路上，"改写后的体在二次分发里仍然是那个体"
是它的前提；前提没钉住就动 D9，将来出问题会分不清是映射写错还是体在重投递时丢了。

补一句免得读串：括号里那几项除语义缓存外都已落地——traceId 传播与 TTFT 见 D8 与 R9，虚拟模型名与
计量在本批（R19–R21），traceId 唯一没结的是**日志 MDC** 那半条（D8 记为已知缺口，第五节那行
instrumentation 是它的候选解）。本批新增的 R12–R21 与 D9/D10 全是未实现状态，实施顺序见第七节
分期表第六到第九期；上面「等计量层落地时用桩钉」那句里的计量层，现在有了编号，就是 R12。

## 七、分期

先跑通最小闭环，再补弹性与灰度。

| 期 | 内容 | 完成标志 | 状态 |
|---|---|---|---|
| 一 | R1 + R2 + R7，单厂商，租户 key 来自配置 | OpenAI 兼容客户端把 `base-url` 指向网关后，非流式与流式都通；无 key 返回 401 | 已完成，见上表 |
| 二 | R4 超时预算 + R11 空闲保护 + R9 观测 | 504 与流中途断开的边界在指标里看得见 | 已完成（traceId 传播 + TTFT 见 D8；日志 MDC 是记录的缺口） |
| 三 | R8 限流 + R6 熔断与 failover | 打满配额返回 429；主厂商挂掉自动切备用 | 已完成（两维限流 + 改道，边界见 D5/D6） |
| 四 | R3 模型路由与灰度 | 按 `model` 分流，权重切换不产生半处理连接 | 已完成（含真机验证），见 D7 |
| 五 | 剩余两条实测回填（熔断回跳与 Weight） | 两条都结论化进本表 | **已完成**。Weight 那半条由 `ModelRoutingTest#weightAndABodyReadingPredicateCoexistOnTheSameGroup` 与 `#theBodyStillReachesTheProviderByteForByteAfterThePredicateReadsIt` 钉着；熔断那半条原先只有源码级结论（`enableBodyCaching(routeId)`），现在 `CircuitBreakerFailoverTest#deadPrimaryIsServedByTheBackup` 补了一条 body 逐字节断言，并用哨兵值验过它真的会红（不是空断言）。R10 的租户热更已在二期前测过 |
| 六 | D10 的 providers 段与路由生成 + D9 的请求侧 model 改写 | 备份关系不再手写；三条顺序约束各有单测；改道后模型名换成备用厂商的真名 | 已完成（真机 `ProviderLiveTest` 3 条走的就是生成路由；跨厂商改道的 `unsupported model` 用桩证伪，机制与剩余未验项见 D9/D10） |
| 七 | R12 计量 + R13 结构化日志 + R14 语义失败 + R15 探活恢复 | usage 四个数在指标与日志里可见；200-with-error-body 会被计进熔断并改道；熔断打开后能自动收拢 | 部分完成：R12、R13 已落地（见 D11），R14 已落地（见 D12）；只剩 R15 探活与自动恢复未做 |
| 八 | R16 密钥池 + R17 令牌治理 + R18 余额探活 | 一把 key 被上游限流不拖累另一把；未授权模型 403 且不碰厂商；余额 gauge 可见 | 未做 |
| 九 | R20 虚拟模型名与升档重试 + R21 跨协议入口 | 一个对外名字后面挂多家、失败换档；三种入口进同一家出且 TTFT 不恶化 | 未做。R21 需要 D1 的第二个例外，届时单独成决策 |

第六期先做而不做第七期，是因为六期的产物是七期的载体：usage 与语义失败都要按 provider 归因，而 provider 这个概念现在散在 metadata、路由 id 和 fallbackUri 三处。顺序反过来会写两遍。
