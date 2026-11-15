# 网关线程模型：反应式栈为什么不需要业务线程池

承接 `requirements.md` 的边界与 `litellm-alignment-design.md` 的决策。这份文档回答两个问题：
网关的线程到底怎么跑，以及为什么它不像经典 Netty 服务那样需要一池业务线程。

**结论先行**：区别不在「调 HTTP 还是写业务」，而在「等结果时线程阻不阻塞」。网关调大模型走的是
端到端非阻塞 I/O，线程发出请求后不等响应、立刻回去服务别的连接，所以不需要额外的业务线程池。
还要先纠正一个措辞：网关不是「没有线程池」，reactor-netty 的 EventLoopGroup 本身就是个线程池，
只是它小到核数级别、且绝不被阻塞，所以不必再叠一个业务池。

---

## 一、三条线程 lane

| lane | 线程 | 承载 | 能否阻塞 |
|---|---|---|---|
| 请求路径 | reactor-netty event loop（`max(4, CPU 核数)`，入站 server + 出站 client 共享默认 `LoopResources`） | 整条过滤器链、路由、鉴权、限流、key 选取、观测、SSE 转发 | **绝不** |
| 共享状态 I/O | Lettuce/Redis 自己的 netty event loop | 限流脚本、token 记账、R25 冷却/开闸读写 | 非阻塞（reactive） |
| 治理调度 | Spring `@Scheduled` 调度线程（默认单线程池） | 探活、余额、熔断同步 | **唯一允许** `.block()` |

### 1. 请求路径：reactor-netty event loop
入站 WebFlux server 与出站网关 HttpClient 共用 reactor-netty 默认的 `LoopResources`（worker 数
`max(4, 核数)`，本项目未自定义）。整条过滤器链都跑在这上面：

```
RequestMetrics(HIGHEST) → ResponseObservation(HIGHEST+1) → TenantAuth(HIGHEST+10)
→ FirstToken(-2) → RetryAfter(0)
→ 路由过滤器(Retry / CircuitBreaker / ProviderKey / ModelMapping / SetPath)
→ TokenQuota(95) → ProviderQuota(100) → RouteToRequestUrl(10000) → NettyWriteResponse
```

全程返回 `Mono/Flux` 链式拼接，主代码里**没有任何 `publishOn/subscribeOn/boundedElastic`**——
即没有把活儿甩到弹性线程池，纪律就是「event loop 上不阻塞」。SSE 正文也是 event loop 直接从
上游连接搬给客户端。

**这条链上的过滤器有两种接口**——`GatewayFilter` 与 `GlobalFilter`，本项目两种都在用：

| | `GatewayFilter` | `GlobalFilter` |
|---|---|---|
| 作用范围 | 单条路由，按路由配置挂载 | 所有路由，全局生效 |
| 怎么来 | 由 `GatewayFilterFactory` 生产 | 注册成 Spring Bean，实现 `GlobalFilter` 接口 |
| 配置方式 | 路由定义里的 `filters: [...]`（或生成器挂上去） | 无需配置，容器里有就生效 |
| 本项目 | `ModelMappingGatewayFilterFactory`、`ProviderKeyGatewayFilterFactory`——改模型名、换厂商 key，只挂需要的厂商路由 | 鉴权/观测/限流：`TenantAuthFilter`、`RequestMetricsFilter`、`ResponseObservationFilter`、`FirstTokenMetricsFilter`、`RetryAfterFilter`、`TokenQuotaFilter`、`ProviderQuotaFilter` |

运行时两者**统一成一条 `GatewayFilter` 链**：`FilteringWebHandler` 把每个 `GlobalFilter` 用
`GatewayFilterAdapter` 包成 `GatewayFilter`，再和路由自带的 `GatewayFilter` 合并、按 `Order` 一起
排序。所以上面那条链能把全局过滤器（`TenantAuth=HIGHEST+10`、`TokenQuota=95`）和路由过滤器
（`ProviderKey`、`ModelMapping`）混排——适配后它们就是同一条链上的节点，也都跑在同一组 event loop 上。
严格说 SCG 对外暴露的是两种编程接口，底层执行时只有一种 `GatewayFilter` 链。

### 2. 共享状态 I/O：Lettuce/Redis event loop
限流脚本、token 记账、R25 冷却/开闸读写都走 `ReactiveStringRedisTemplate`。写侧一律
fire-and-forget（`.onErrorResume(...).subscribe()`），调用方立即返回，真正的 I/O 落在 Redis
客户端自己的 event loop 上，不占网关线程。

### 3. 治理调度：`@Scheduled` 单线程
`GatewayApplication` 开了 `@EnableScheduling`，未配 `spring.task.scheduling.pool.size` →
**默认单线程池**。三个定时任务共用这一条线程：
- `ProviderHealthProbe.probe`（30s，`.block(timeout)`）
- `ProviderBalanceProbe`（10m，`.block(timeout)`）
- `SharedBreakerSync.syncOpenBreakers`（5s）

全代码库的 `.block()` **只出现在这三处及其调用链**（探活 WebClient + `SharedState.isOpen`），
全部在调度线程，绝不碰请求路径。

---

## 二、硬约束：请求路径绝不阻塞

这是整个模型的地基，R25 把它钉得最死。`ProviderKeyGatewayFilterFactory.apply` 在 **event loop 上
同步**调 `keyPool.pick()` → `isCooling()`，所以同一个 `SharedState` 的两个读方法有两种线程语义：

- **冷却读** `SharedState.isCooling`：必须非阻塞。本地短 TTL 缓存新鲜就答、过期就答旧值并**异步刷新**，
  绝不在 event loop 上等 Redis。跨实例可见性至多滞后一个 `local-cache-ttl`（D21）。
- **开闸读** `SharedState.isOpen`：可以有界 `.block(500ms)`，因为它**只被调度线程**
  （`SharedBreakerSync`）调用。

按「谁在什么线程上调用」来分阻塞与否，而不是按「读什么」——这是反应式栈里最容易踩错的地方。
观测侧 `ResponseObservationFilter` 的 `doFinally`（结算 token、冷却 key、记指标）跑在响应完成/取消
的那条线程（通常 event loop），里面 settle/coolDown 也都是非阻塞的。

---

## 三、与 jaws（Netty I/O 线程 + 业务线程池）的对比

### jaws：I/O 线程 + 业务线程池
```
BossGroup     → accept 连接
WorkerGroup   → 读写 socket、编解码（EventLoop，一个线程绑一组连接，串行）
业务线程池     → 执行 handler 里的接口逻辑（JDBC / 同步 RPC / 同步 HTTP …）
```
**为什么必须分出业务池**：Netty 的 EventLoop 是「单线程串行处理名下所有连接」。一旦 handler 里做了
阻塞操作，这条 EventLoop 线程就被钉死，它名下成百上千条连接全部停摆。所以必须把阻塞活儿 offload
到业务线程池，让 EventLoop 马上回去服务别人。

**但 jaws 的业务池是 thread-per-blocking-work，不是 thread-per-request**——后者是对它最常见的误描述：
- **服务端内部早已全链路异步**：三种传输统一 `serverExecutor.execute()` 卸载后走 `handleAsync()`；
  `DefaultProvider.invoke` 对返回 `CompletableFuture` 的异步 provider 只做 `orTimeout` 包装，业务线程
  派发完立即释放，并不独占请求全程——框架自己并不为等 I/O 而 park。
- **真正 park 线程的是消费端同步调用**：`DefaultResponseFuture.getValue()` 委托 `CompletableFuture.get()`，
  调用线程被挂起直到结果返回。这一侧才是 thread-per-request。
- **线程数也不随并发线性增长**：`AbstractNettyServer` 的业务池是 bounded + `AbortPolicyWithStats`
  fail-fast，封顶在 `maxWorkerThreads`，满了直接拒绝，而不是按并发数开线程。

所以 jaws 的池子存在是为了隔离「用户可能阻塞的业务代码」，而不是框架自己在等 I/O 时 park。按下文的
分水岭理论，这恰恰印证了它设计的合理性：阻塞活儿被甩出去，I/O 线程保持非阻塞。

### reactor-netty：只有 event loop
```
EventLoopGroup（max(4,核数)，入站 + 出站共享）
   → 整条过滤器链都在这上面跑，没有业务池
```
**为什么敢这么干**：链上没有任何阻塞调用。调大模型用 reactor-netty 的异步 HttpClient——发请求后
线程不等，而是把「这条**上游连接**可读时叫我」注册到 epoll/kqueue，然后返回。上游字节到了，EventLoop 被
唤醒，接着跑后面的 reactive 链把正文搬给客户端。本质是 **event-loop 多路复用**：线程从不为等 I/O
而 park，几条线程同时照看成千上万条连接。

**两条连接都在 epoll 上**：注册的不止上游这一条——客户端 → 网关的下游连接由 reactor-netty 的
HttpServer 管理，同样注册在 event loop 上，且入站 + 出站共享同一个 EventLoopGroup。一次转发实际
涉及两条连接、两个 epoll 注册项：
- 下游连接可读 → EventLoop 读到客户端请求 → 跑过滤器链 → 异步发给上游；
- 上游连接可读 → EventLoop 读到响应字节 → 沿 reactive 链搬回下游连接写给客户端。

上面 HttpClient 那段强调的只是**出站方向不阻塞**：请求发出去后线程不等上游回话，回去服务别的连接；
把正文搬给客户端是另一段事，属于上游可读事件触发之后的 reactive 链。

### 关键澄清：不是「调 HTTP 就不用线程池」
最常见的误解。**如果用阻塞式 HTTP 客户端**（`HttpURLConnection`、Apache HttpClient 同步 `execute`、
OkHttp 同步 `call`）在 EventLoop 上去调大模型，一样会把 EventLoop 钉死——LLM 一个回答动辄几秒到
几十秒，SSE 流式更是整段流期间线程都动不了。这时照样需要业务线程池，而且因为 LLM 慢、并发高，
池子得开得巨大。真正的分水岭：

| | 阻塞式调用 | 非阻塞式调用 |
|---|---|---|
| 等上游时线程在干嘛 | park 挂起，独占一个线程 | 不等，回去服务别的连接 |
| 需要的线程数 | ≈ 并发请求数 | ≈ CPU 核数（几个 EventLoop） |
| 要不要业务线程池 | 要（保护 EventLoop） | 不要 |

网关落在右列，是因为它端到端非阻塞：reactor-netty 异步 client + WebFlux reactive 链 + Lettuce
reactive Redis + 非阻塞计时器。请求路径里一个 `.block()` 都没有，唯一的阻塞被圈进 `@Scheduled`
调度线程，那几条线程不在转发路径上。

---

## 四、为什么 LLM 网关特别适合非阻塞

大模型调用的特征是**高延迟、长连接、流式 SSE、I/O 密集但 CPU 轻**：
- 阻塞模型：一个流式回答 30s 就占一个业务线程 30s，1 万并发 → 1 万线程，内存和上下文切换双爆。
- 非阻塞模型：1 万条 SSE 连接只是 1 万个 epoll 注册项 + 少量缓冲区，几个 EventLoop 轮询就绪事件
  即可，**线程数不随并发线性增长**。

这正是反应式栈的主场，也是网关选它的根本原因。

---

## 五、超时/看门狗也在 event loop 层

- **空闲看门狗** `UpstreamIdleTimeoutCustomizer`：给每条上游 channel 挂一个 Netty
  `ReadTimeoutHandler`（per-channel、带计时状态，不能共享），在 event loop 上计时，连续 idle
  没读到字节就抛 `ReadTimeoutException` 断连。它管「已经开始回话之后多久没新字节」，与路由
  metadata 的 `response-timeout`（管「等多久算厂商没回响应头」）不是一回事（R11）。
- `response-timeout`（首字节预算）、`connect-timeout`（per-route）同样是 reactor-netty 的非阻塞计时。

---

## 六、代价与纪律

非阻塞不是白来的：
- **代码必须 reactive**（`Mono/Flux` 链），不能写「看着同步」的阻塞代码。一颗老鼠屎（一个 `.block`、
  一次 JDBC）就能钉死 EventLoop、坏一锅汤——这也是为什么本网关把阻塞严格隔离到调度线程。
- 栈追踪/调试不直观（异步回调链）。
- **CPU 密集活儿仍会占 EventLoop**（大 JSON 解析、精确 tokenizer 计数）。真要重活得甩到
  `boundedElastic`。本网关这类活儿很轻（`ModelsController` 纯内存构建清单），所以没引入弹性池。

两个值得知道的特性（都不是请求路径问题）：
1. **调度池单线程**：三个定时任务串行，探活/余额是顺序 `.block(timeout)`。厂商多且都超时时，一轮
   探活可能占住这唯一线程数秒，把 `syncOpenBreakers` 往后推。影响的是治理时效，不影响转发。要提速
   可配 `spring.task.scheduling.pool.size`。
2. **`ModelsController`** 是 `@RestController`，在 event loop 上同步构建清单——但纯内存、无 I/O、
   模型量小，可忽略。

---

一句话总结：**请求转发全程在 reactor-netty event loop 上非阻塞流动，Redis I/O 在 Lettuce 自己的
loop 上异步跑，所有阻塞被圈进单线程的 `@Scheduled` 调度 lane。** jaws 用业务线程池隔离用户可能阻塞的
业务代码、保护 I/O 线程（其服务端框架自身已全链路异步）；网关不用，是因为它从编程模型上就禁止阻塞，
I/O 线程自己就能安全地跑完业务逻辑。差别是阻塞 vs 非阻塞，不是 HTTP vs 业务。