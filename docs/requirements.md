# AI 网关需求

基于 Spring Cloud Gateway 的大模型网关：对外提供 OpenAI 兼容接口，直连多家模型厂商，负责路由、鉴权、限流、熔断与观测。

## 一、边界

网关只负责把一次模型调用送到该去的厂商，并把响应原样流回来。四件事不在范围内：

- 计费与配额结算。网关留下取用量的钩子和埋点，不落库、不出账。
- 语义缓存与对话缓存。需要向量检索，属另一层。
- 内容安全与敏感词。需要策略库与合规口径。
- Prompt 编排、Agent、工具调用。属应用层。

## 二、需求

验收标准是这一层的核心：每条需求都写成能被一次请求证伪的形式。

| # | 需求 | 验收标准 | Spring Cloud Gateway 5.0.2 落点 | 状态 |
|---|---|---|---|---|
| R1 | OpenAI 兼容入口 | 客户端只改 `base-url` 与 `model` 就能打下游多家；`/v1/chat/completions`、`/v1/embeddings`、`/v1/models` 三条路径可用 | `Path` + `Method` 谓词，`SetPath` / `RewritePath` | 内置够用 |
| R2 | 厂商适配 | 一个请求只落一个厂商端点，网关不改写请求体与响应体 | 路由选择上游 host，`SetRequestHeader` 换 `Authorization`，`SetPath` 归一路径 | 见决策 D1 |
| R3 | 模型路由与灰度 | 按请求体 `$.model` 选厂商；同一模型可按权重切流量 | `ReadBody` 谓词（JSONPath + SpEL），`Weight` 谓词 | 内置够用 |
| R4 | 流式与超时 | 上游不回响应头时按预算返回 504；持续吐字的长回答不被总时长切断 | `routes[n].metadata.response-timeout`（毫秒） | 见决策 D2 |
| R5 | 失败重试 | 只对尚未开始下发的请求重试；重试不重复计费 | `Retry` 过滤器 | 见决策 D3 |
| R6 | 熔断与降级 | 厂商错误达阈值后短路，切备用厂商或返回明确降级 | `CircuitBreaker` 过滤器 + `fallbackUri: forward:...` 引到备用路由 | 已实现，见决策 D6 |
| R7 | 鉴权与租户隔离 | 无 key、错 key、已吊销 key 一律 401；租户身份贯穿日志、指标、限流键 | 自研 `GlobalFilter`，排在路由之前 | 自研 |
| R8 | 限流 | 按租户与按厂商两条维度独立限；被限返回 429 带 `Retry-After`，且请求不碰到厂商 | 租户走 `RequestRateLimiter` + 自定义 `KeyResolver`；厂商走自研 `ProviderQuotaFilter` | 已实现，见决策 D5 |
| R9 | 可观测 | 每个请求产出 `tenant / provider / route / 状态 / 耗时` 指标；运维面不与业务端口共用 | 自建 `ai.gateway.requests` Timer + actuator 单独端口 | 已实现；traceId 与 TTFT 未做 |
| R10 | 配置动态下发 | 改 Nacos 配置后路由、权重、密钥即时生效，不重启不断流 | `RouteRefreshListener` 监听 `RefreshScopeRefreshedEvent` | 已实测，见决策 D4 |
| R11 | 流中途空闲保护 | 厂商开始流式之后停止吐字，网关在空闲预算内断开，不挂死连接 | `HttpClientCustomizer` 给上游通道加 `ReadTimeoutHandler` | 已实现，见决策 D2 |

## 三、决策

六条现在不定、以后要重写的判断。

### D1 网关不改写报文

`spring-cloud-gateway-server-webflux` 5.0.2 的 `filter/factory` 包里没有 `ModifyRequestBodyGatewayFilterFactory` 与 `ModifyResponseBodyGatewayFilterFactory`，只有 `CacheRequestBodyGatewayFilterFactory` 和 `RemoveJsonAttributesResponseBodyGatewayFilterFactory`。改写请求体要自己写 `ServerHttpRequestDecorator`，改写响应体在流式下意味着逐块解析再重组，首字延迟直接被做掉。

所以适配只做三件事：选上游 host、换 `Authorization`、归一 path。厂商私有协议（例如 DashScope 原生 `/api/v1/services/aigc/...`）不在这一层接，要接就用它的 OpenAI 兼容端点。需要往请求体注入字段时（例如 `stream_options`），要求客户端自带，网关不代劳。

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
文档里那条「`ReadBody` 能不能重读」的未测卡点就是这个。

四件事必须记下来：

1. **`CircuitBreaker` 要排在 `SetPath` / `SetRequestHeader` 之前。** 它改道时用的是「自己看到的那个 request」；厂商 key 若已被前面的过滤器换进去，重投递时鉴权读到的是厂商 key，直接 401。这条是第一版实测撞出来的。
2. **`resilience4j.timelimiter` 的默认 `timeout-duration` 是 1 秒。** spring-cloud-circuitbreaker 会把每次调用包在 TimeLimiter 里，大模型没有 1 秒回来的调用，于是**每个请求都被判超时走 fallback**。真机上表现为：qwen 请求正常返回，客户端却拿到 DeepSeek 回的 400。生产配置里显式给了 120s，与路由的 `response-timeout` 对齐；`CircuitBreakerFailoverTest#slowPrimaryIsNotMistakenForADeadOne` 钉住这条。
3. **只有 connect 级失败能被客户端感知到改道。** 端口没人听 → 客户端拿到备用的 200。而「连上了、请求写出去了、然后 5xx / 被 reset / 流到一半断」这三类，熔断器会计数、备用厂商也会被敲，但客户端拿到的还是主厂商那个失败——响应已经开始走，替换不了。这未必是坏事：请求已经进了厂商，改道可能把同一次计费算两遍。
4. **跨厂商改道带着一个模型名问题。** 请求体里的 `model` 是原厂商的名字，改道后新厂商多半不认识——真机实测：`qwen3.7-plus` 被改道到 DeepSeek，回来的是 `The supported API model names are deepseek-flash, deepseek-v4-pro`。要按厂商改写 `model` 就得动报文，与 D1 相冲；这件事和 R3 的「虚拟模型名 → 各厂商真实模型名」是同一个问题，留到四期一起解。

还有一条环境性的：这台机器上「连一个没人听的端口」经网关要耗约 10 秒才失败（`curl` 直连是 0.2 毫秒），所以熔断用例的滑动窗口设成 2、真连死端口的用例只留两条，否则一个测试类要跑两分钟。

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
                - Path=/deepseek/v1/chat/completions   # 客户端只改 base_url 的路径前缀
              metadata:
                provider: deepseek
                response-timeout: 120000
                connect-timeout: 3000
              filters:
                - SetPath=/v1/chat/completions
                - SetRequestHeader=Authorization,Bearer ${DEEPSEEK_API_KEY}
```

自定义段用顶层 `ai-gateway` 前缀，不挤进 `spring.*`。`routes` 段走 SCG 原生绑定，`RouteRefreshListener` 已经在监听 `RefreshScopeRefreshedEvent`，Nacos 改配置触发 spring-cloud-context 刷新，路由随之重建，R10 不需要代码。

一期实现的是 `tenants` 加手写 `routes`；上面的 `providers` 段是目标形状，等厂商数量多到路由表手写不动了再引入 `@ConfigurationProperties` 与路由生成，那时密钥池（同一家多把 key 轮转）一并解决。

## 五、依赖缺口

`gateway/pom.xml` 现在有 `spring-cloud-starter-gateway-server-webflux`、`spring-cloud-starter-alibaba-nacos-config`、`spring-cloud-starter-circuitbreaker-reactor-resilience4j`（R6）、`spring-boot-starter-data-redis`（R8 的桶状态）、`spring-boot-starter-actuator` + `micrometer-registry-prometheus`（R9）、`netty-resolver-dns-native-macos`（macOS 上缺它启动就一直在打 `Unable to load io.netty.resolver.dns.macos...` 的 ERROR），外加测试依赖。按需求清单还缺：

| 依赖 | 服务的需求 | 说明 |
|---|---|---|
| `spring-boot-starter-opentelemetry` | R9 的 traceId | 指标已有，链路还没有 |
| `spring-cloud-starter-alibaba-nacos-discovery` + `spring-cloud-starter-loadbalancer` | 内网私有化模型 | 只有接 vLLM / Ollama 集群才需要，直连公网厂商用不上 `lb://` |

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
| 熔断打开后不再敲主厂商 | `CircuitBreakerFailoverTest#breakerOpensAfterAWindowOfFailures` 与 `#openBreakerStillServesTheBackupWithoutWaitingOnThePrimary`：窗口 2、阈值 50%，打开之后那一次只打备用 |
| 慢的主厂商不该被当成死的 | `CircuitBreakerFailoverTest#slowPrimaryIsNotMistakenForADeadOne`：1.5 秒回响应头，改道次数为 0。去掉 `resilience4j.timelimiter...timeout-duration` 这条就红——默认 1 秒会把每个模型调用判超时 |
| 5xx / reset / 流中途断这三种改不了道 | `#serverErrorFromALivePrimaryIsNotReplacedByTheBackup`、`#connectionResetAfterTheRequestWasSentStillReturnsThePrimaryFailure`、`#midStreamFailureCannotBeFailedOver`：熔断计数与备用都被敲，客户端仍拿主厂商的结果 |
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

还剩两条没测，因为它们各自的实现还没写：

1. `Weight` 谓词与 `ReadBody` 组合在同一路由组上的匹配顺序，以及切权重时是否有连接被半处理。

（原先列在这里的另一条——`fallbackUri` 二次进入路由链时请求体能不能重读——已由 D6 回答：配了
`fallbackUri` 之后 `CircuitBreaker` 自己会 `enableBodyCaching(routeId)`。）

## 七、分期

先跑通最小闭环，再补弹性与灰度。

| 期 | 内容 | 完成标志 | 状态 |
|---|---|---|---|
| 一 | R1 + R2 + R7，单厂商，租户 key 来自配置 | OpenAI 兼容客户端把 `base-url` 指向网关后，非流式与流式都通；无 key 返回 401 | 已完成，见上表 |
| 二 | R4 超时预算 + R11 空闲保护 + R9 观测 | 504 与流中途断开的边界在指标里看得见 | 已完成；traceId 与 TTFT 留给 R9 的下一段 |
| 三 | R8 限流 + R6 熔断与 failover | 打满配额返回 429；主厂商挂掉自动切备用 | 已完成（两维限流 + 改道，边界见 D5/D6） |
| 四 | R3 模型路由与灰度 | 按 `$.model` 分流，权重切换不产生半处理连接 | 未做 |
| 五 | 剩余两条实测回填（熔断回跳与 Weight） | 两条都结论化进本表 | 未做；R10 的租户热更已在二期前测过 |
