# SillyTavern 断线续传方案

## 1. 问题分析

### 当前架构

```
客户端                         服务器                        LLM API
  │                             │                             │
  │  POST /generate (stream:true)│                             │
  │ ─────────────────────────► │                             │
  │                             │  POST /v1/chat/completions  │
  │                             │ ──────────────────────────► │
  │                             │                             │
  │  ◄── SSE stream (chunks) ── │  ◄── SSE stream (chunks) ── │
  │                             │                             │
  │  [客户端断线]               │                             │
  │  socket.close()            │                             │
  │ ─────────────────────────► │                             │
  │                             │  AbortController.abort()   │
  │                             │ ──────────────────────────► │
  │                             │  [请求被中断，数据丢失]     │
  │                             │                             │
  │  [客户端重连后]             │                             │
  │  ── 没有任何方式恢复 ──     │                             │
```

### 关键代码位置

**服务端（Express）：**
- [`src/endpoints/backends/chat-completions.js`](../src/endpoints/backends/chat-completions.js) - 主生成路由 `/generate`
  - 第2548-2552行：创建 AbortController 并在 socket close 时中断请求
  - 第2607-2611行：调用 `forwardFetchResponse` 将流式响应转发给客户端
- [`src/util.js`](../src/util.js) - `forwardFetchResponse` 函数 (第718-765行)
  - 第749行：`from.body.pipe(to)` 将上游流管道到下游
  - 第752-756行：在客户端 socket close 时销毁上游流并结束响应

**客户端：**
- [`public/scripts/openai.js`](../public/scripts/openai.js) - `sendOpenAIRequest` 函数 (第3078-3151行)
  - 使用 `fetch` 的 `signal` 参数来支持客户端取消
  - 通过 `EventSourceStream` 解析 SSE 流
- [`public/script.js`](../public/script.js) - `sendStreamingRequest` 函数 (第6121-6138行)

### 支持的后端

所有聊天补全 API 都有相同的 socket close → abort 模式：
- OpenAI/OpenRouter/Custom (第2548-2552行)
- Claude (第226-230行)
- Google/Gemini (第638-642行)
- DeepSeek (第1045-1049行)
- xAI (第1157-1161行)
- Azure OpenAI (第1700-1702行)
- 以及其他（Mistral, Cohere, AI21 等）

### 其他后端

- [`src/endpoints/backends/text-completions.js`](../src/endpoints/backends/text-completions.js) - Text Completions API
- [`src/endpoints/backends/kobold.js`](../src/endpoints/backends/kobold.js) - Kobold API
- [`src/endpoints/novelai.js`](../src/endpoints/novelai.js) - NovelAI API

---

## 2. 设计方案

### 核心思路

将客户端连接与 LLM API 请求**解耦**：

1. 为每个生成请求分配唯一 ID
2. 客户端断线时**不取消** LLM API 请求，而是将收到的 chunk 缓冲到内存中
3. 客户端重连后，通过请求 ID 查询并继续接收缓冲的流数据
4. 提供完整的 API 让客户端可以查询未完成的请求、获取已完成的最终结果

### 架构图

```
客户端                         服务器                        LLM API
  │                             │                             │
  │  POST /generate             │                             │
  │  x-request-id: uuid        │                             │
  │ ─────────────────────────► │                             │
  │                             │  生成 requestId             │
  │                             │  注册到 RequestRegistry     │
  │                             │  POST /v1/chat/completions  │
  │                             │ ──────────────────────────► │
  │                             │                             │
  │  ◄── SSE stream (chunks) ── │  ◄── SSE stream (chunks) ── │
  │                             │  同时缓冲到内存缓冲区       │
  │                             │                             │
  │  [客户端断线]               │                             │
  │  socket.close()            │                             │
  │ ─────────────────────────► │                             │
  │  [服务器不中断请求]        │                             │
  │  [继续缓冲 chunk]          │  ◄── SSE stream (chunks) ── │
  │                             │                             │
  │  [客户端重连]              │                             │
  │  GET /api/request/:id      │                             │
  │ ─────────────────────────► │                             │
  │  ◄── 已缓冲的 chunks ──── │                             │
  │  ◄── 继续 SSE stream ──── │  ◄── SSE stream (chunks) ── │
  │                             │                             │
```

### 关键组件

#### 2.1 RequestRegistry（请求注册表）

**文件**: [`src/request-registry.js`](../src/request-registry.js)（新建）

内存中的请求会话管理器，负责：

```javascript
class RequestRegistry {
  // Map<requestId, RequestSession>
  sessions: Map<string, RequestSession>

  // 创建新请求会话
  createSession(requestId, requestBody): RequestSession

  // 获取现有会话
  getSession(requestId): RequestSession | undefined

  // 追加缓冲区数据
  appendBuffer(requestId, chunk): void

  // 标记完成
  markComplete(requestId, finalData?): void

  // 标记失败
  markFailed(requestId, error): void

  // 清理过期会话
  cleanup(): void
}
```

**RequestSession** 结构：

```typescript
interface RequestSession {
  requestId: string;           // 唯一请求 ID
  createdAt: number;           // 创建时间戳
  lastActiveAt: number;        // 最后活跃时间
  status: 'streaming' | 'completed' | 'failed';
  buffer: string[];            // 已缓冲的 SSE 数据事件（限制最大条数）
  finalData?: any;             // 完成时的最终数据
  error?: any;                 // 失败时的错误信息
  abortController: AbortController; // 允许手动取消
  userId: string;              // 关联用户（多用户隔离）
}
```

#### 2.2 请求 ID 生成

两种方案（推荐**方案A**）：

**方案 A：服务端生成，通过响应头返回**
- 客户端发起请求时**不提供** requestId
- 服务端在请求处理开始时生成 UUID
- 通过 `x-request-id` 响应头返回给客户端
- 客户端在收到第一个响应头时提取并保存
- 优点：向后兼容，现有客户端无需修改即可获得 requestId

**方案 B：客户端生成，通过请求头发送**
- 客户端在发起请求时生成 UUID
- 通过 `x-request-id` 请求头发送
- 服务端使用该 ID 注册会话
- 优点：客户端更早知道 ID
- 缺点：需要修改客户端代码

#### 2.3 缓冲区设计

- 每个会话维护一个环形缓冲区，存储 SSE 事件字符串
- 最大容量：**10,000 条事件**（约 5-10MB，取决于 token 长度）
- 达到上限时丢弃最早的 chunk（FIFO）
- 每个 chunk 带时间戳，用于去重

#### 2.4 新 API 端点

##### `POST /api/backends/chat-completions/generate`（修改现有）

**新增行为**：
- 为每个请求生成 `requestId`（UUID v4）
- 在响应头中返回 `x-request-id: <uuid>`
- 将请求注册到 `RequestRegistry`
- 不再在 socket close 时 abort 请求（改为将请求标记为「离线流模式」）
- 将收到的 SSE chunk 同时写入响应流和缓冲区

**响应头**：
```
x-request-id: abc-123-def-456
x-request-status: streaming
```

##### `GET /api/backends/chat-completions/request/:requestId`（新增）

**功能**：让断线重连的客户端继续接收流式响应

**行为**：
- 查找 `requestId` 对应的会话
- 如果会话不存在 → 返回 404
- 如果会话已完成 → 返回完整结果（JSON，非流式）
- 如果会话正在流式传输中：
  - 设置 SSE 响应头
  - 先发送已缓冲的所有事件
  - 然后继续转发新到达的事件（使用长连接）
  - 当会话最终完成时，发送 `[DONE]` 事件并结束响应

**请求**：
```
GET /api/backends/chat-completions/request/abc-123-def-456
```

**响应**（流式 SSE，与原始格式相同）：
```
event: message
data: {"choices":[{"delta":{"content":"Hello"}}]}

event: message
data: {"choices":[{"delta":{"content":" world"}}]}

event: message
data: [DONE]
```

##### `POST /api/backends/chat-completions/request/:requestId/abort`（新增）

**功能**：让客户端手动取消进行中的请求

**行为**：
- 查找 `requestId` 对应的会话
- 调用会话的 `abortController.abort()`
- 返回成功状态

#### 2.5 服务端修改清单

##### A. 修改 `forwardFetchResponse` ([`src/util.js`](../src/util.js#L718))

**当前行为**（第752-756行）：
```javascript
to.socket.on('close', function () {
    if (from.body instanceof Readable) from.body.destroy();
    to.end();
});
```

**修改为**：
- 不再在 socket close 时销毁 upstream
- 当有 `requestId` 上下文时，仅将状态标记为「离线」
- 继续消费 upstream stream 并写入缓冲区
- 当无 `requestId` 时保持原有行为（向后兼容）

##### B. 修改 `/generate` 路由 ([`src/endpoints/backends/chat-completions.js`](../src/endpoints/backends/chat-completions.js#L2166))

**当前第2548-2552行**：
```javascript
const controller = new AbortController();
request.socket.removeAllListeners('close');
request.socket.on('close', function () {
    controller.abort();
});
```

**修改为**：
```javascript
const controller = new AbortController();
const requestId = uuidv4(); // 生成唯一 ID

// 注册会话
const session = requestRegistry.createSession(requestId, request.body, controller);

// 设置响应头
response.setHeader('x-request-id', requestId);

// 不再在 socket close 时 abort
// 改为：socket close 时标记为离线，继续缓冲
request.socket.removeAllListeners('close');
request.socket.on('close', function () {
    session.markClientDisconnected();
    // 注意：不调用 controller.abort()！
});

// 稍后在 forwardFetchResponse 中：同时写入 response 和 buffer
```

##### C. 修改 `forwardFetchResponse` 调用方式

目前的调用：
```javascript
await forwardFetchResponse(generateResponse, response);
```

修改为支持缓冲区：
```javascript
await forwardFetchResponse(generateResponse, response, { 
    buffer: session, 
    requestId: requestId 
});
```

##### D. 处理相同 API 的多个函数

需要对以下函数应用相同的模式（每个函数都有 socket.close → abort 逻辑）：

| 函数 | 文件 | 行号 | 状态 |
|------|------|------|------|
| `sendClaudeRequest` | chat-completions.js | 226-230 | ✅ 需修改 |
| `sendMakerSuiteRequest` | chat-completions.js | 638-642 | ✅ 需修改 |
| `sendAI21Request` | chat-completions.js | 776-779 | ✅ 需修改 |
| `sendMistralAIRequest` | chat-completions.js | 858-861 | ✅ 需修改 |
| `sendCohereRequest` | chat-completions.js | 939-942 | ✅ 需修改 |
| `sendDeepSeekRequest` | chat-completions.js | 1045-1049 | ✅ 需修改 |
| `sendXaiRequest` | chat-completions.js | 1157-1161 | ✅ 需修改 |
| `sendAimlapiRequest` | chat-completions.js | 1263-1267 | ✅ 需修改 |
| `sendElectronHubRequest` | chat-completions.js | 1368-1372 | ✅ 需修改 |
| `sendChutesRequest` | chat-completions.js | 1480-1484 | ✅ 需修改 |
| `sendMinimaxRequest` | chat-completions.js | 1582-1586 | ✅ 需修改 |
| `sendAzureOpenAIRequest` | chat-completions.js | 1700-1702 | ✅ 需修改 |
| 主 `/generate` handler | chat-completions.js | 2548-2552 | ✅ 需修改 |
| `sendTextCompletionRequest` | text-completions.js | ? | ❓ 待确认 |
| `sendKoboldRequest` | kobold.js | ? | ❓ 待确认 |

**优化方案**：不逐个修改每个函数，而是创建一个**统一的中断处理包装器**：

```javascript
// 新工具函数：创建断线续传的 AbortController
function createResilientAbortController(request, response, requestRegistry) {
    const controller = new AbortController();
    const requestId = uuidv4();
    
    // 注册会话（仅在流式请求时）
    if (request.body.stream) {
        const session = requestRegistry.createSession(requestId, request.body, controller);
        response.setHeader('x-request-id', requestId);
        
        request.socket.removeAllListeners('close');
        request.socket.on('close', () => {
            session.markClientDisconnected();
            // 不 abort LLM 请求，继续缓冲
        });
        
        return { controller, requestId, session };
    }
    
    // 非流式请求保持原有行为
    request.socket.removeAllListeners('close');
    request.socket.on('close', () => controller.abort());
    return { controller, requestId: null, session: null };
}
```

#### 2.6 缓冲区持久化与清理

##### 清理策略
- 已完成/失败的会话：**5 分钟后**清理
- 离线流式会话：**30 分钟后**清理（假设客户端不可能在 30 分钟内重连）
- 使用 `setInterval` 每 60 秒运行一次清理

##### 内存限制
- 缓冲区最大事件数：10,000 条/会话
- 最大活跃会话数：20 个（超过时拒绝新请求）
- 配置项（config.yaml）：
  ```yaml
  requestRegistry:
    maxSessions: 20
    maxBufferSize: 10000
    sessionTTL: 300000  # 5分钟
    offlineSessionTTL: 1800000  # 30分钟
    cleanupInterval: 60000  # 1分钟
  ```

#### 2.7 客户端修改

##### A. 保存 requestId

在 `sendOpenAIRequest` ([`public/scripts/openai.js`](../public/scripts/openai.js#L3088)) 中，从响应头提取并保存：

```javascript
const response = await fetch(generate_url, { ... });
const requestId = response.headers.get('x-request-id');
// 保存到持久存储（localStorage 或 sessionStorage）
if (requestId) {
    sessionStorage.setItem(`pending_request_${requestId}`, JSON.stringify({
        requestId,
        timestamp: Date.now(),
        chatId: getCurrentChatId(),
        // 其他上下文
    }));
}
```

##### B. 断线检测与重连

SillyTavern 已有 `setOnlineStatus` 机制（[`public/script.js`](../public/script.js#L806)）。在断线重连时：

1. 检查 `sessionStorage` 中是否有未完成的请求
2. 对于每个未完成的请求，调用 `GET /api/backends/chat-completions/request/:requestId`
3. 如果返回 404（请求已清理），正常处理
4. 如果返回流式响应，重新连接 Stream 并继续接收

```javascript
// 断线重连时恢复流
async function reconnectPendingRequests() {
    const keys = Object.keys(sessionStorage);
    for (const key of keys) {
        if (!key.startsWith('pending_request_')) continue;
        const pending = JSON.parse(sessionStorage.getItem(key));
        
        try {
            const response = await fetch(`/api/backends/chat-completions/request/${pending.requestId}`);
            if (response.ok && response.headers.get('content-type')?.includes('text/event-stream')) {
                // 重新连接流
                const eventStream = getEventSourceStream();
                response.body.pipeThrough(eventStream);
                // ... 继续消费
            }
        } catch (err) {
            console.warn('Failed to reconnect pending request:', err);
        } finally {
            sessionStorage.removeItem(key);
        }
    }
}
```

##### C. StreamingProcessor 修改

[`StreamingProcessor`](../public/script.js#L3540) 需要能处理「重新连接」模式：
- 不从零开始创建 generator
- 从已缓冲的 chunks 恢复

---

## 3. 实施步骤

### 阶段一：核心服务端改动

| 步骤 | 文件 | 改动内容 | 优先级 |
|------|------|---------|--------|
| 1 | [`src/request-registry.js`](../src/request-registry.js) | 新建文件：RequestRegistry 和 RequestSession 类 | P0 |
| 2 | [`src/util.js`](../src/util.js) | 修改 `forwardFetchResponse`：支持可选 buffer 参数，不在 client disconnect 时销毁 upstream | P0 |
| 3 | [`src/endpoints/backends/chat-completions.js`](../src/endpoints/backends/chat-completions.js) | 新建 `createResilientAbortController` 工具函数；修改 `/generate` 路由 | P0 |
| 4 | [`src/endpoints/backends/chat-completions.js`](../src/endpoints/backends/chat-completions.js) | 新增 `GET /request/:requestId` 和 `POST /request/:requestId/abort` 路由 | P0 |
| 5 | [`src/server-startup.js`](../src/server-startup.js) | 将 RequestRegistry 模块注册到 app（如果需要全局访问） | P0 |

### 阶段二：客户端改动

| 步骤 | 文件 | 改动内容 | 优先级 |
|------|------|---------|--------|
| 6 | [`public/scripts/openai.js`](../public/scripts/openai.js) | 在 `sendOpenAIRequest` 中提取并保存 x-request-id | P0 |
| 7 | [`public/script.js`](../public/script.js) | 添加 `reconnectPendingRequests` 函数；在断线重连时调用 | P0 |
| 8 | [`public/script.js`](../public/script.js) | 修改 `StreamingProcessor` 以支持断线重连场景 | P0 |

### 阶段三：扩展至其他后端

| 步骤 | 文件 | 改动内容 | 优先级 |
|------|------|---------|--------|
| 9 | [`src/endpoints/backends/text-completions.js`](../src/endpoints/backends/text-completions.js) | 应用相同的 resilience 模式 | P1 |
| 10 | [`src/endpoints/backends/kobold.js`](../src/endpoints/backends/kobold.js) | 应用相同的 resilience 模式 | P1 |
| 11 | [`src/endpoints/novelai.js`](../src/endpoints/novelai.js) | 应用相同的 resilience 模式 | P1 |

### 阶段四：清理与测试

| 步骤 | 文件 | 改动内容 | 优先级 |
|------|------|---------|--------|
| 12 | [`src/request-registry.js`](../src/request-registry.js) | 实现定期清理逻辑 | P0 |
| 13 | [`default/config.yaml`](../default/config.yaml) | 添加 RequestRegistry 配置项 | P1 |
| 14 | 测试 | 模拟断线重连场景 | P0 |

---

## 4. 边界情况与注意事项

### 4.1 重复事件处理
- 客户端可能在重连时收到已经处理过的 chunk
- 解决方案：客户端维护 `lastEventId`，服务端支持 `Last-Event-ID` 头部（SSE 原生支持），从指定位置开始发送

### 4.2 多设备/多标签页
- 同一个用户可能在不同标签页发起多个请求
- 解决方案：requestId 本身就是唯一的，按 ID 索引即可
- 如果多个标签页想「接续」同一个请求，需要额外的用户确认机制（暂不支持）

### 4.3 安全性
- 请求 ID 是 UUID v4，难以猜测
- 但仍然需要验证请求 ID 是否属于当前用户
- 在 `GET /request/:id` 中添加用户身份验证

### 4.4 非流式请求
- 非流式请求不需要 resilience（响应是瞬时的）
- 仅在 `request.body.stream === true` 时启用

### 4.5 服务端重启
- 内存中的缓冲区会在服务端重启后丢失
- 这是一个折中方案，因为磁盘持久化会增加复杂性和延迟
- 未来可以考虑使用 Redis 或 SQLite 作为可选的持久化后端

### 4.6 前端中止（Stop Button）
- 用户点击「停止」按钮时，应该中止 LLM 请求
- 当前：客户端的 `abortController.abort()` 导致 fetch 中断，进而触发 socket close
- 修改后：需要区分「用户主动停止」和「网络断开」
- 方案：停止按钮调用 `POST /request/:id/abort`（而不是直接 abort fetch）
- 如果直接 abort fetch，socket close 仍会触发，但服务器不中断 LLM 请求（因为断线续传模式）

---

## 5. 关键实现细节

### 5.1 forwardFetchResponse 修改

```javascript
export async function forwardFetchResponse(from, to, options = {}) {
    let statusCode = from.status;
    let statusText = from.statusText;
    
    // ... 状态码处理 ...
    
    if (from.body && to.socket) {
        from.body.pipe(to);
        
        // 如果提供了 bufferSession，将数据同时写入 buffer
        if (options.buffer) {
            from.body.on('data', (chunk) => {
                const chunkStr = chunk.toString('utf-8');
                // 解析 SSE 事件并存储到 buffer
                options.buffer.appendChunk(chunkStr);
            });
        }
        
        to.socket.on('close', function () {
            // 如果没有 requestId（传统模式），销毁 upstream
            if (!options.requestId) {
                if (from.body instanceof Readable) from.body.destroy();
                to.end();
            }
            // 有 requestId 时：不销毁 upstream，继续缓冲
            // 客户端可以通过 GET /request/:id 重新连接
        });
        
        from.body.on('end', function () {
            console.info('Streaming request finished');
            if (options.buffer) {
                options.buffer.markComplete();
            }
            to.end();
        });
    }
}
```

### 5.2 流重新连接的实现

```javascript
router.get('/request/:requestId', async (request, response) => {
    const { requestId } = request.params;
    const session = requestRegistry.getSession(requestId);
    
    if (!session) {
        return response.status(404).send({ error: 'Request not found' });
    }
    
    // 验证用户身份
    if (session.userId !== request.user?.id) {
        return response.status(403).send({ error: 'Forbidden' });
    }
    
    if (session.status === 'completed') {
        // 已完成：直接返回最终数据
        return response.json(session.finalData);
    }
    
    if (session.status === 'failed') {
        // 已失败：返回错误
        return response.status(500).send({ error: session.error });
    }
    
    // 流式进行中：设置为 SSE
    response.setHeader('Content-Type', 'text/event-stream');
    response.setHeader('Cache-Control', 'no-cache');
    response.setHeader('Connection', 'keep-alive');
    response.setHeader('x-request-id', requestId);
    
    // 发送已缓冲的所有事件
    for (const bufferedEvent of session.getBufferedEvents()) {
        response.write(bufferedEvent);
    }
    
    // 注册回调以接收新事件
    const onChunk = (chunk) => {
        response.write(chunk);
    };
    
    const onComplete = (finalData) => {
        if (finalData) response.write(finalData);
        response.write('data: [DONE]\n\n');
        response.end();
    };
    
    const onError = (error) => {
        response.end();
    };
    
    session.on('chunk', onChunk);
    session.on('complete', onComplete);
    session.on('error', onError);
    
    // 客户端再次断开时清理
    response.on('close', () => {
        session.off('chunk', onChunk);
        session.off('complete', onComplete);
        session.off('error', onError);
        
        if (session.status === 'streaming') {
            session.markClientDisconnected();
            // 仍然不 abort LLM 请求
        }
    });
});
```

### 5.3 EventEmitter 模式用于会话通知

RequestSession 需要继承 EventEmitter（或使用简单回调）来通知新 chunk：

```javascript
class RequestSession extends EventEmitter {
    // ...
    
    appendChunk(chunkStr) {
        this.buffer.push(chunkStr);
        // 限制缓冲区大小
        if (this.buffer.length > this.maxBufferSize) {
            this.buffer.shift(); // 丢弃最早的
        }
        this.emit('chunk', chunkStr);
    }
    
    markComplete(finalData) {
        this.status = 'completed';
        this.finalData = finalData;
        this.emit('complete', finalData);
    }
    
    markFailed(error) {
        this.status = 'failed';
        this.error = error;
        this.emit('error', error);
    }
}
```

---

## 6. 时间线/优先级

- **P0**（必须实现）：阶段一（步骤 1-5）、阶段四的步骤 12
- **P0**（必须实现）：阶段二的步骤 6-7（保存 requestId + 断线重连）
- **P1**（重要但可延后）：阶段二的步骤 8、阶段三（其他后端）、配置项
- **P2**（未来增强）：多标签页支持、磁盘持久化、非流式请求 resilience

---

## 附录 A：引用文件

| 文件 | 说明 |
|------|------|
| [`src/endpoints/backends/chat-completions.js`](../src/endpoints/backends/chat-completions.js) | 主聊天补全 API 路由，包含所有后端的 `/generate` 端点 |
| [`src/util.js`](../src/util.js) | `forwardFetchResponse` 函数，处理流式响应管道 |
| [`src/server-startup.js`](../src/server-startup.js) | Express 路由注册 |
| [`public/scripts/openai.js`](../public/scripts/openai.js) | 客户端的 OpenAI/Chat Completions 请求处理 |
| [`public/scripts/sse-stream.js`](../public/scripts/sse-stream.js) | 客户端的 SSE 流解析 |
| [`public/script.js`](../public/script.js) | 主客户端脚本，包含 StreamingProcessor 和断线检测 |
| [`public/scripts/kai-settings.js`](../public/scripts/kai-settings.js) | Kobold 客户端流式请求 |
| [`public/scripts/nai-settings.js`](../public/scripts/nai-settings.js) | NovelAI 客户端流式请求 |
| [`public/scripts/textgen-settings.js`](../public/scripts/textgen-settings.js) | Text Generation 客户端流式请求 |
| [`src/endpoints/backends/text-completions.js`](../src/endpoints/backends/text-completions.js) | Text Completions 后端 |
| [`src/endpoints/backends/kobold.js`](../src/endpoints/backends/kobold.js) | Kobold 后端 |
| [`src/endpoints/novelai.js`](../src/endpoints/novelai.js) | NovelAI 后端 |
