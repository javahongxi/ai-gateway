# ai-gateway

基于 Spring Cloud Gateway 的大模型网关：对外一个 OpenAI 兼容入口，按配置把请求路由到多家模型厂商。

需求、决策与实测记录见 [docs/requirements.md](docs/requirements.md)。

## 跑起来

上游厂商 key 从环境变量读，不进仓库：

```
export DASHSCOPE_API_KEY=sk-...
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

没有 key 时请求不会碰到厂商，按 OpenAI 的错误信封返回 401：

```
{"error":{"message":"Missing API key: send an Authorization: Bearer <gateway key> header.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}
```

## 测试

```
mvn -pl gateway test
```

上游是进程内桩，不联网，7 个用例覆盖鉴权、租户隔离、凭证注入、报文原样、流式逐块与超时语义。真机冒烟单独跑，缺 `DASHSCOPE_API_KEY` 自动 skip：

```
mvn -pl gateway test -Dtest=DashScopeLiveTest
```
