# 对标 LiteLLM Proxy 的四件补强：设计

承接 `requirements.md` 的边界与决策。本批补四件 LiteLLM proxy 有、本网关缺的能力：

| # | 需求 | 一句话 | 默认 |
|---|---|---|---|
| R22 | `/v1/models` 聚合 | 网关自己报出对外可售的模型清单，不透传厂商 | 开（仅生成路由启用时） |
| R23 | token 级限流 | 租户 TPM：请求前按 `max_tokens` 预扣、响应后按真实 usage 结算 | 关（配额 0） |
| R24 | 首字节前请求内重试 | 连接级失败时同一请求内重试，不必等熔断改道治后续请求 | 关（次数 0） |
| R25 | 冷却/熔断跨实例共享 | 多实例共享 key 冷却与熔断开闸信号 | 关 |

四件都**加法式**落地：除 R22 外默认关闭，打开前现有行为与测试一字不变。这是刻意的——
`requirements.md` 里那些结论都是踩坑换来的，新功能不该在没被显式启用时就改动它们。

边界仍然不变（`requirements.md` 第一节）：计量在内、结算在外；不做语义缓存、内容安全、Prompt 编排。
R23 只到"按 token 限流"，不到"按 token 扣费"——它读 usage 是为了回灌限流器，不是为了算钱。

---

## R22 `/v1/models` 聚合

### 现状与缺口
生成器（`ProviderRouteDefinitionLocator`）目前只产 chat 路由，`Provider` 只有 `chat-path`。
R1 声称的 `/v1/models` 实际没有。OpenAI SDK 的 `models.list()` 打过来是 404。LiteLLM 的
`/v1/models` 聚合所有对外 `model_name`，`/model/info` 再给上下文窗口/价格——本网关只做前者。

### 机制
**网关本地聚合，不透传厂商。** 对外可售的名字有两个来源：
- `model-groups` 的虚拟名（如 `chat-default`）——一个名字挂多家，`owned_by` 记 `ai-gateway`；
- 各厂商 `model-mapping` 的**键**（客户端-facing 名字）——`owned_by` 记该厂商别名。

厂商 `models` 里的通配（`deepseek*`）是**路由模式**不是可枚举的模型名，不进清单。
按 `id` 去重，组名优先。输出 OpenAI 形状：`{"object":"list","data":[{"id","object":"model","created","owned_by"}]}`。

### 决策 D18：走 forward 路由，鉴权不绕过
不做裸 `@RestController`。裸控制器走 `RequestMappingHandlerMapping`（order 0），会**绕过整条
网关过滤器链**——`TenantAuthFilter` 是 `GlobalFilter`，只对路由请求生效，裸控制器等于把模型清单
匿名公开。模型清单（有哪些厂商、哪些模型）是轻度敏感信息，不该匿名可读。

所以生成一条路由 `models-list`：`Path=/v1/models` + `Method=GET`，`uri: forward:/_gateway/models`，
由一个 `@RestController`（`ModelsController`，挂在内部前缀 `/_gateway/` 下，与 `/backup/`、`/internal/`
同类，不是对外入口）产出清单。这样请求照常过鉴权、租户限流、访问日志、指标——`provider` 维度记 `none`。
控制器每次请求**现读** `AiGatewayProperties`，Nacos 改完即时反映，不依赖路由重建。

清单构建抽成静态纯函数 `ModelsCatalog.build(providers, groups)`，单测不起上下文（与生成器 `build` 同套路）。

### 配置
无新配置项。`generated-routes=true` 且有路由生成时附带产出 `models-list`；关掉生成器则没有这条路由。

### 验收
- 带合法租户 key `GET /v1/models` → 200，`data` 含组名与各厂商映射键，`object=list`。
- 不带 key → 401，且与 chat 入口同一套错误信封。
- 清单随 `providers`/`model-groups` 声明变化（纯函数单测）。
- 通配模式（`deepseek*`）不出现在清单里。

---

## R23 token 级限流（预扣 + 结算）

### 现状与缺口
租户维度现在是 SCG `RequestRateLimiter` 的**请求数**令牌桶（`requestedTokens=1`），
`application.yml` 注释自己留了口子"想按 token 量扣就调大"——但那是静态值，不是按每次调用真实用量。
LiteLLM `parallel_request_limiter_v3` 的形状是：**请求前按 `max_tokens` 预扣 TPM → 响应后按真实
usage 结算差额**。本网关已有 R12 的 usage 采集，把"事后采到的数"回灌限流器就是它的形状。

### 范围
本批只做**租户 TPM**（最清楚的缺口：单个调用方不能无限烧 token）。厂商 TPM 留作后续——
机制（`TokenQuotaService` 的预扣/结算）是可复用的，加一维只是多一个键前缀。
prompt token 的精确预扣需要 tokenizer（jtokkit），属新依赖，与"R12–R20 不新增依赖"的约束相冲，
本批**只预扣 `max_tokens`**（completion 的有界部分），prompt 计在结算里。这条取舍写进 D19。

### 机制
新增 `TokenQuotaService`（Redis + 注入 `Clock` + properties）与 `TokenQuotaFilter`（GlobalFilter，order 95，
紧邻 `ProviderQuotaFilter` 的 100，在鉴权之后、上游之前）。固定窗口，与厂商维度同形状（D5）：

- **预扣**（请求阶段）：窗口键 `ai-gateway:tokens:tenant:<租户>:<窗口序号>`。
  预扣额 = 请求体 `max_tokens`（>0 时），否则 `default-token-reservation`。
  `INCRBY key 预扣额`，若累计 > 配额 → `INCRBY key -预扣额` 退回并 429；否则放行，
  把预扣额/窗口键/配额写进 exchange 属性（**请求阶段写**，外层读得到，D11）。
- **结算**（响应阶段）：挂在 `ResponseObservationFilter.settle()` 里——它已经拿到 usage 与 tenant，
  且预扣属性是请求阶段写的、读得到。不另起观察者，不跨完成回调传属性（D11）。
  - 2xx 且有 usage：`INCRBY key (prompt+completion − 预扣额)`（cached/reasoning 是子集，不重复计）。
  - 2xx 但无 usage：保留预扣额，不退（保守）。
  - 非 2xx（上游失败/504）：`INCRBY key -预扣额` 全额退回（这次调用没产出计费 completion）。
  - 被本过滤器自己 429 的：没写预扣属性，结算什么都不做。
- **Redis 不可达放行**（与厂商维度同取舍，D5）：预扣那一次 `onErrorResume` 放行、不写属性；
  结算 fire-and-forget。把配额存储的可用性乘进网关可用性不值当。

429 带 `Retry-After`（窗口剩余秒）+ `X-Tenant-TokenLimit-Limit` 头，OpenAI 错误信封
`code=tenant_token_limit_exceeded`。`RetryAfterFilter` 的兜底不覆盖已给过的值。

### 决策 D19：预扣 max_tokens、结算回灌、失败放行
- 只预扣 `max_tokens`：它是请求里有界的、网关读得到的部分；prompt 精确计数要 tokenizer，留后续。
- 结算复用 `ResponseObservationFilter`：一份正文读一次（D11），预扣属性请求阶段写所以读得到。
- 非 2xx 退预扣、2xx-无-usage 保留：前者"没发生"，后者"发生了但量未知"，宁可多限不可少限。
- Redis 失败放行：与厂商维度一致，这一维是软保护。

### 配置（`ai-gateway.rate-limit.*`）
```yaml
tenant-tokens-per-window: 0      # 租户每窗口 token 上限，0=关（默认）
token-window-seconds: 60         # 窗口长度
default-token-reservation: 1024  # 请求没带 max_tokens 时的预扣额
```

### 验收（需本机 Redis，缺则整类 skip）
- 带 usage 的调用：预扣 60，结算回灌成真实用量 18（11+7），窗口计 18 而非停在 60。
- 无 usage 时保留预扣（保守）：两发各 60 叠到 120 > 100，第二发 429、上游只收到 1 次；
  超限退回后窗口仍是第一发的 60，不是 120。
- 预扣额取 `max_tokens`；没带时取 `default-token-reservation`。
- 上游回非 2xx → 预扣额全额退回（窗口计数归零），下一发照样放得出去。
- 429 带 `Retry-After` 与 `X-Tenant-TokenLimit-Limit`，错误码 `tenant_token_limit_exceeded`。
- 配额 0（默认）时整条路径不读体、不碰 Redis，现有行为不变。
- 结算是 fire-and-forget 异步写：断言窗口计数用轮询等它落地，窗口本身由固定时钟钉死。

---

## R24 首字节前请求内重试

### 现状与缺口
R5 写了"只对尚未开始下发的请求重试"，但 D3 指出内置 `Retry` 默认 `methods=[GET]`，POST 等于没配——
实际没启用。D17 的模型组是"每跳一条路由 + 熔断改道"，治的是**后续请求**。缺的是：
**同一请求内**对连接级失败重试几次，且不依赖"必须配了 backup"。LiteLLM `num_retries` 就是这一层，
与熔断改道互补。

### 机制
用**内置 `Retry` 过滤器**，由生成器在 `retries>0` 时挂到对外/组路由上。不新造一套路由器内重试
（D17 的告诫：别在已验证的 forward 机制旁边再放一个没验证过的）。保守配置：
- `methods=[POST]`（D3：不改 methods 等于没配）；
- `exceptions=[java.io.IOException, java.util.concurrent.TimeoutException]`（`ConnectException` 是
  `IOException` 子类）——只重试**连接级/响应头前**的异常；
- `statuses=[]`、`series=[]`——**不**按 HTTP 状态重试：5xx 已带响应体，重试会重复计费（D3/D6.3）；
- 退避 `firstBackoff=50ms, maxBackoff=500ms, factor=2`。

流中途断（`PrematureCloseException` during write）在写响应侧冒出、绕过 Retry 作用域（D6.3 已证），
所以"只重试首字节前"是这个配置的天然结果，不是额外约束。

### 决策 D20：内置 Retry、默认关、只重试连接级异常
- 默认 `max-attempts=0`：不挂 Retry，现有行为不变。
- 只重试异常不重试状态：守住"响应开始下发后不重试"，避免两份 token 拼给同一客户端。
- 与熔断共存：Retry 挂在路由过滤器链最前（在 CircuitBreaker 之外），每次重试仍走熔断与改道；
  主备都连不上时重试耗尽才失败。请求体由 Model 谓词缓存过、可重放，连接失败时体未被上游消费。

### 配置（`ai-gateway.retry.*`）
```yaml
max-attempts: 0        # 额外重试次数，0=关（默认）
first-backoff: 50ms
max-backoff: 500ms
```

### 验收
- 桩"第一发连接级失败、之后正常"：`max-attempts=1` → 客户端 200、桩收到 2 次；
  `max-attempts=0` → 客户端 500、桩收到 1 次。
- 5xx（已带响应体）不触发重试（桩只收到 1 次）。
- 默认 0 时生成路由不含 Retry 过滤器（纯函数单测）。

---

## R25 冷却/熔断跨实例共享

### 现状与缺口
`UpstreamKeyPool` 的冷却是 `ConcurrentHashMap`（JVM），resilience4j 熔断也是 JVM 状态，只有限流桶在 Redis。
多实例时"A 实例判死的厂商/ key，B 实例还在敲"。LiteLLM 用 `coordination_redis` 把 cooldown/health 共享。

### 机制（`ai-gateway.shared-state.enabled`，默认 false）
新增 `SharedState`（Redis + 本地短 TTL 缓存兜底），两类信号：

**(a) key 冷却——共享，选取侧非阻塞读。** `UpstreamKeyPool.coolDown` 在本地冷却表之外，同时
fire-and-forget 写 Redis 键 `ai-gateway:cooldown:<provider>:<指纹>`（TTL=冷却时长）。`isCooling`
先查本实例本地冷却表，再查 `SharedState` 的本地短 TTL 缓存：缓存新鲜直接用，过期就**异步刷新、本次
返回上次已知值**——因为 `pick` 跑在 reactor 线程上，绝不能为它阻塞 Redis。跨实例可见性至多滞后一个
`local-cache-ttl`。指纹口径不变（末四位，原文绝不进 Redis 键）。Redis 失败退回本地冷却（fail-open，与 D5 同）。

**(b) 熔断开闸——定时同步，只开不关。**
- 写：给 `CircuitBreakerRegistry` 注册 `onEntryAdded → onStateTransition` 监听，本地 breaker
  →OPEN 时写 `ai-gateway:breaker:<provider>`（TTL=wait-duration-in-open-state），→CLOSED 时删。
- 读：`SharedBreakerSync` `@Scheduled`（间隔 `sync-interval`，默认 5s）扫各 provider 的远端开闸标记，
  若标记在而本地 breaker 非 OPEN → `transitionToOpenState()`。**只开不关**：收闸仍归本地探活
  （R15/D13）与半开，避免两个观察者抢一台状态机（D12 的教训）。
- 探活收闸时（`ProviderHealthProbe`）顺手 `markClosed` 清远端标记。

### 决策 D21：冷却共享、开闸定时、默认关、只开不关
- 冷却按 key 共享：写实时（fire-and-forget），读非阻塞（本地短 TTL 缓存 + 异步刷新，每次选取都查、
  缓存降压），跨实例可见至多滞后 `local-cache-ttl`——这是不阻塞 reactor 线程换来的。开闸状态秒级定时
  同步，比实时 pub/sub 简单，代价是至多 `sync-interval` 的延迟，写进文档不藏着。
- 只开不关：远端信号只触发"开"，"关"留给本地探活/半开，守住 D12/D13 的单一写者原则。
- 默认关：关闭时 `UpstreamKeyPool` 走纯本地（现状），不注册监听、不起同步、不碰 Redis。
- Redis 失败一律退回本地行为，绝不把 Redis 可用性乘进网关可用性。

### 配置（`ai-gateway.shared-state.*`）
```yaml
enabled: false         # 默认关
sync-interval: 5s      # 开闸标记同步间隔
local-cache-ttl: 2s    # 远端标记的本地缓存，降每请求 Redis 压力
```

### 验收（需本机 Redis，缺则整类 skip）
- 两个各带自己 `SharedState` 的 `UpstreamKeyPool` 共享同一 Redis：A 冷却某 key，B 的 `isCooling`
  轮询后为 true、选取时让开那把；TTL 到期后 false。
- `SharedState.markOpen` 后 `isOpen` 为 true；`markClosed` 后 false；TTL 自然过期。
- 本地熔断 →OPEN 写远端标记、→CLOSED 删（监听写侧）。
- 定时同步**只开不关**：远端标记在 → 开本地闸；远端清了 → 本地仍 OPEN（收闸归探活/半开）。
- 探活治愈：本地收闸 + 顺手清远端开闸标记。
- 默认关时 `SharedState` 全程不碰 Redis（传 null redis 也不 NPE）、`UpstreamKeyPool` 纯本地
  （现有 `UpstreamKeyPoolTest` 不改仍绿）。
- Redis 不可达：写不抛错、没本地登记过的 key 远端读为 false、本实例本地冷却照常。

---

## 实施顺序与依赖

1. **R22**（最独立）：`ModelsCatalog` + `ModelsController` + 生成器加 `models-list` 路由。
2. **R23**：`TokenQuotaService` + `TokenQuotaFilter` + `ResponseObservationFilter` 结算钩子 + 配置。
3. **R24**：配置 + 生成器挂 Retry + 桩加"先失败后成功"场景。
4. **R25**：`SharedState` + `UpstreamKeyPool` 集成 + `SharedBreakerSync`（监听写侧 + 定时同步读侧）
   + 探活钩子 + 配置。

不新增 Maven 依赖（Redis、resilience4j、Jackson 都已在树里）。R23 的 tokenizer、R25 的实时 pub/sub
是明确的后续项，不在本批。

## 测试约定（沿用现有）
- 每个新测试类一句自己的 `ai-gateway.test-context` 标记（上下文缓存键只看合并配置）。
- 依赖 Redis 的类 `@BeforeAll` 调 `RateLimitTestSupport.requireLocalRedis()`，缺则整类 skip。
- 固定 `Clock` 走 `@TestConfiguration @Primary`，真实时间不参与断言。**例外**：R25 共享类靠异步缓存
  刷新与 Redis 真实 TTL，两者无法用注入时钟伪造，故用有界轮询等状态稳定（短 TTL + 4s 上限）。
- 不测限流的类 `@Import(AlwaysAllowRateLimiter.class)`。
- 纯函数（清单构建、预扣额计算）单测不起上下文。