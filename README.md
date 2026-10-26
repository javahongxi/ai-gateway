# ai-gateway

基于 Spring Cloud Gateway 的大模型网关：对外一个 OpenAI 兼容入口，按配置把请求路由到多家模型厂商。

需求、决策与实测记录见 [docs/requirements.md](docs/requirements.md)。

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
curl -s http://127.0.0.1:8089/actuator/prometheus | grep ai_gateway_requests_seconds_count
```

```
ai_gateway_requests_seconds_count{application="gateway",provider="dashscope",route="dashscope-chat",status="200",tenant="team-a"} 2
ai_gateway_requests_seconds_count{application="gateway",provider="dashscope",route="dashscope-chat",status="401",tenant="anonymous"} 1
```

被鉴权挡掉的请求同样计数，且排在鉴权之前——否则「某租户一直拿错 key 打网关」在指标里是隐身的。SCG 自带的 `http_client_requests_seconds` 只有 route 维度，没有租户与厂商，这是自建一条指标的理由。

上游流中途卡住由 `ai-gateway.upstream.idle-timeout` 兜（默认 60s）：路由 metadata 的 `response-timeout` 只管「等响应头」，正文阶段没有超时，见 `docs/requirements.md` 决策 D2。

## 测试

```
mvn -pl gateway test
```

12 个用例走进程内桩，不联网：10 个覆盖鉴权、租户与厂商隔离、两条厂商路由各自的凭证与路径、报文原样、流式逐块、超时语义与指标维度；2 个覆盖空闲看门狗（切断卡住的流、不误伤持续吐字的流）。真机冒烟在 `ProviderLiveTest`，按每家的环境变量单独开关，缺哪个 skip 哪个：

```
mvn -pl gateway test -Dtest=ProviderLiveTest
```
