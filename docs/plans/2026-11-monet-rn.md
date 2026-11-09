# monet-rn：React Native 集成（jsi-turbo TurboModule + WHATWG fetch TS 层）

## Context

transport 层验证矩阵已闭合（2026-11）：Android 真机电池双绿（rustls + 系统信任库）、iOS 模拟器电池全绿（23 项 + demo App 13 项）。monet-rn 是最后一块：**jsi-turbo `#[RustTurboModule]` 包装 monet-core Client**，JS 侧暴露 WHATWG fetch 兼容子集 + WebSocket，验证形态是真实 RN App——顺带覆盖遗留的 iPhone 真机验证。

定位回顾（DESIGN.md）：monet-core 零 RN/JSI 依赖；monet-rn 是前端适配层，平台 transport 选择 iOS=URLSession（原生）、Android=monet-rustls + monet-ws。

## 已核实的关键事实（决定设计形状，2026-11）

### jsi-turbo 能力面（`jsi-turbo/docs/usage.md` + 源码核实）

- `#[RustTurboModule("X")]` 单宏 = 全部 spec：注册、TS 绑定生成、平台构建钩子全自动
- `async fn` → Promise：body 跑 worker 池（futures-lite），**签名不得含 JSI 类型**（HRTB 约束，编译期拒绝）
- `TurboCallback`：JS 函数作参数，`Send + Sync` 可长存，任意线程随时调，经 CallInvoker 跳回 JS 线程；**参数 serde 序列化、fire-and-forget 无返回值**（错误吞掉，`Vec<u8>` → JS number 数组，大块二进制不可走）
- `JsiArrayBuffer`：`from_vec` 零拷贝（所有权转移，GC 回收时才 drop），**仅同步方法可用**
- `TurboStruct`：serde 值语义，可跨 async 边界；`Option<T>`/`Vec<T>` 支持嵌套
- smol 家族 crate 可直接依赖（async-io 全局 reactor 线程驱动唤醒，与 worker 池共存）——jsi-turbo 内置 runtime 刻意最小（无 reactor/timer/IO）

### 所有 monet transport 已是 runtime-agnostic BoxFuture

monet-rustls / monet-ws 内部私有单线程 tokio runtime（"callers poll plain futures and the channel-based Body, never tokio types"）；monet-urlsession 零 tokio（std watcher 线程 + futures-channel）。**monet-rn 不需要持有任何 tokio runtime**：顶层 future 交给 `async_global_executor::spawn`（smol 系）即可，与 worker 池共存（jsi-turbo 文档明示此组合方式）。

### build.js 聚合构建的路径约束（源码核实）

- crate 按 `node_modules/<pkg>/<crateDir>` 解析；聚合 superlib 的依赖是**相对路径文本连接**（symlink 不解析——jsi-rs example 的 `jsi-demo-module -> ../native/jsi-demo-module` 实证 npm `file:` 安装产生 symlink，但 crate 内依赖仍按 node_modules 路径文本连接到兄弟包）
- 由此推出两条硬约束：
  1. **jsi/jsi-turbo 依赖**：crate 放 npm 包根 → `../react-native-rust-turbo/rust/...` 解析到 node_modules 兄弟包 ✓（jsi-demo-module 同款）
  2. **monet crate 依赖**：相对路径出不了包（file: 装进 node_modules 后 `../../crates/...` 落到 app 目录下，不存在）→ **monet 系 crate 必须 vendor 进 npm 包内**
- git 依赖方案否决：本地未提交改动进不了 git dep，开发迭代不可用；registry 方案留待发布期

### 流式协议形状（nitro `requestStatic` 同款 + 零拷贝）

async fn 不能收 JSI 类型 + TurboCallback 不能走大块二进制 → 流式 = **「回调通知 + 同步方法零拷贝取块」**：

- 数据到达时 `on_chunk` 回调只发信号（可带小元数据），JS 再调同步方法 `take_chunk(id)` 拿 `JsiArrayBuffer`（Rust 侧队列 → JS 零拷贝）
- 上行（request body / ws send）：同步方法收 `JsiArrayBuffer`（JS → Rust 零拷贝借用），v1 上传全缓冲（与 monet-cronet v1、nitro 上传语义一致）

## 定稿：monet-rn 包结构与模块 API

```
packages/monet-rn/                # npm 包根 = crate 根（crate 放包根，jsi 兄弟路径才通）
  package.json                    # main: src/index.ts
  │   rust-turbo: { crate: ".", crateName: "monet-rn", modules: ["Monet"] }
  │   files: [src, ts, vendor, Cargo.toml]   # cargo 源随包分发（聚合构建在消费方 app 内跑）
  Cargo.toml                       # 独立 crate（[workspace] 空表隔离，同 jsi-demo-module 模式）
  │   jsi = { path = "../react-native-rust-turbo/rust/jsi", features = ["macros","serde"] }
  │   jsi-turbo = { path = "../react-native-rust-turbo/rust/jsi-turbo" }
  │   monet-core = { path = "./vendor/monet-core" }   # vendor 内相对路径
  │   [target.'cfg(any(target_os="ios", target_os="macos"))'.dependencies] monet-urlsession
  │   [target.'cfg(target_os="android")'.dependencies] monet-rustls, monet-ws
  │   async-global-executor, futures-lite, serde, linkme, anyhow
  src/lib.rs                       # #[RustTurboModule("Monet")]（唯一模块）
  src/fetch.rs                     # fetch 通道：fetchId 注册表 + pump 任务 + 取块队列
  src/ws.rs                        # WS 连接对象（#[TurboObject] + NativeState，Drop = 断开）
  ts/                               # 手写 TS 层（WHATWG fetch 兼容子集）
  │   index.ts                     # 导出 fetch/MonetWebSocket/configurePins/…
  │   response.ts                  # Response 封装（ok/status/headers/arrayBuffer/text/json/body 异步迭代器）
  vendor/                          # monet crate 的 vendored 副本（单一事实源在 workspace crates/）
  scripts/sync-vendor.sh           # rsync crates/monet-{core,tls,rustls,ws,urlsession} → vendor/（--delete 保持同步）
  generated/                       # 宏生成 TS（gitignore）
```

vendor 同步是开发节奏的一部分：改 crates/ 后跑一次 sync，再构建 app（jsi-turbo 对框架 crate 本身也是"每次构建从 checkout 同步"的同一模式）。单一事实源保持在 workspace crates/（host 测试 + 一致性电池在那里跑）。

### 模块 API（v1，全同步方法 + TurboCallback + 零拷贝取块）

```rust
#[derive(TurboStruct)] pub struct FetchOptions {
    url: String, method: String,
    headers: Vec<HeaderKV>,        // {name, value} 小结构体
    timeout_ms: f64,               // v1.1 起效（需 core per-request 支持，见后）
    follow_redirects: bool,        // 同上
}

#[RustTurboModule("Monet", ctor = "MonetModule::create")]
impl MonetModule {
    // ---- 配置（client 级；重建 transport + Client，RwLock<Client>）----
    pub fn configure_pins(&self, json: String) -> Result<()>   // nitro-fetch-pinning.json 同格式，fail-closed
    pub fn clear_pins(&self) -> Result<()>
    pub fn set_timeout_ms(&self, ms: f64) -> Result<()>        // iOS 落 URLRequest.timeoutInterval

    // ---- fetch：head 到达即回调，body 之后流式 ----
    pub fn fetch(&self, rt, options: FetchOptions, body: Option<JsiArrayBuffer>,
                 on_head: TurboCallback,   // {status, headers: [{name,value}]}
                 on_chunk: TurboCallback, // {} 信号；JS 随后 take_chunk
                 on_done: TurboCallback,
                 on_error: TurboCallback)  // {kind, message}（Cancelled/Transport/Hook/Tls）
        -> Result<f64 /*fetchId*/>
    pub fn take_chunk(&self, rt, fetch_id: f64) -> Result<Option<JsiArrayBuffer>>  // None = 暂无
    pub fn body_done(&self, fetch_id: f64) -> Result<bool>   // EOS 判定（队列空且已结束）
    pub fn abort(&self, fetch_id: f64) -> Result<bool>       // CancelToken.cancel()

    // ---- WebSocket：连接对象（#[TurboObject]），无注册表、无 connId ----
    pub fn ws_connect(&self, url: String, protocols: Vec<String>, headers: Vec<HeaderKV>,
                      on_open: TurboCallback, on_message: TurboCallback, // {isBinary}
                      on_close: TurboCallback,                          // {code, reason, wasClean}
                      on_error: TurboCallback) -> Result<MonetWsConnection>
}

#[TurboObject]
impl MonetWsConnection {
    pub fn send(&self, rt, message: JsiArrayBuffer, is_binary: bool) -> Result<bool>
    pub fn send_ping(&self) -> Result<bool>
    pub fn close(&self, code: f64, reason: String) -> Result<bool>
    pub fn abort(&self) -> Result<bool>
    pub fn take_message(&self, rt) -> Result<Option<JsiArrayBuffer>>  // None = 暂无
}
```

**WS 连接的生命周期 = JS 对象的生命周期**（M4 实施时从注册表方案改过来）：
`ws_connect` 返回的连接对象经 `to_js` 交给 JS，其 NativeState 持唯一强引用；
JS 侧 GC（或模块卸载）→ NativeState 析构 → Rust `Drop` → `CancelToken` →
pump 收尾。pump 只共享可克隆的片段（取消息队列 `Arc<Mutex<Vec>>`、命令通道
接收端），绝不持有连接本身——引用计数归零完全由 JS 驱动。TS 层的四个事件
回调闭包同样**不能捕获 socket 对象**（TurboCallback 的 JSI 句柄是 Hermes 的
强 GC 根；构造器里的箭头函数还会词法捕获 `this`），在模块级工厂里创建、只
捕获相关 id，事件经弱表解析。**释放语义**：丢掉引用即释放——app 自身的分配
驱动回收器走到 wrapper 的 NativeState，Rust `Drop` 断开连接，无需任何显式
dispose；要立即断开用 `close()`（握手）/`abort()`（取消）。**两个引擎事实**
（2026-11 实测钉死）：Hades 的强制 `gc()`（force-compaction 路径）清 WeakRef
但跳过 NativeState cell finalization——`Drop` 只在自然老生代清扫时跑，是
eventually 而非可观测窗口；`WeakRef.deref()` 会把目标放进 Hermes 的
keep-alive 集（keepDuringJob）直到下一次 JSI `drainMicrotasks`——宿主稀疏
drain 时被轮询的对象因自己的探针而永生（所以 TS 层的事件分发 deref 是对的：
事件在流动期间连接本就该活着）。连接握手期间 socket 由 TS 层的 connecting
表强持有：`await open` 的挂起帧对 GC 是无根环
（socket → onopen 闭包 → resolve → promise → 续体 → 环境 → socket），
握手窗口内的一次 GC 会把 socket 连同 open 一起收走。

内部结构：

- **pump 任务**（`async_global_executor::spawn`，每 fetch/每连接一个）：`client.execute_with_cancel(req, token)` → head 到 → `on_head(status, headers)`；随后循环 `resp.body.next()` → 入队 `Mutex<VecDeque<Bytes>>` + `on_chunk()`；`None` → `on_done`，`Err` → `on_error`。fetch 终态后从注册表摘除（take_chunk 的 EOS 用 body_done 区分"队列空"与"已结束"）；WS 无注册表——pump 只握队列与命令通道，终态即返回，状态随连接对象存亡
- **iOS**：`UrlsessionTransport` 一份实例同时服务 HTTP + WS（两 trait 同一类型）；pins 走 `with_pins`（shim 全局注册，last-wins，与 host 语义一致）
- **Android**：`MonetRustlsTransport`（HTTP）+ `MonetWsTransport`（WS）；pins 分别进 `MonetRustlsConfig.pins` / `WsConfig`（`TlsTrust::PlatformVerifier` 默认，Android 系统信任库）

### TS 层（手写，WHATWG fetch 兼容**子集**）

- `fetch(input, init?) → Promise<MonetResponse>`：`init.method/headers/body/signal`（AbortSignal → abort(fetchId)，RN 自带 AbortController）
- `MonetResponse`：`ok/status/headers(Headers)/arrayBuffer()/text()/json()` + `body` 异步迭代器（for-await 逐块）；**不做**：FormData/Blob/ReadableStream 完整规范、redirect 手动模式（v1 一律跟随）
- `MonetWebSocket`：EventTarget 风格（onopen/onmessage/onclose/onerror + addEventListener），binaryType 兼容；内部持有原生连接对象（NativeState）。丢弃不 close 也不泄漏：JS 侧无持有，自然回收触发 Rust `Drop` 断开；`close()`/`abort()` 用于需要立即断开的场景
- 回调 → TS Promise/事件的粘合全部在这一层，生成绑定不外露

### v1 明确不做（记录在案）

- **JS 侧 hook**（设计目标 #2）：需要 TurboCallback 回程协议（on_request → JS → `resolve_request_hook(hookId, modified)` → pending-map oneshot），是可实现的真工作量，排 M5
- per-request timeout/redirect 开关（core Request 无此概念，v1 只有 client 级）
- 上传流式（v1 全缓冲）、响应背压（nitro `Promise<bool>` 协议）、prefetch/token refresh（v2，nitro 对标项）、FormData/Blob
- permessage-deflate（全栈 v1 均不支持，无回退损失）

## 验证形态：platform/rn-demo（RN 0.88 App）

```
platform/rn-demo/                  # in-repo RN App（jsi-rs example 同款骨架）
  package.json                     # react-native 0.88.x + react-native-rust-turbo
  │   + "monet-rn": "file:../../packages/monet-rn"
  App.tsx                          # 电池屏幕：逐项检查、报告列表（与 ios-demo/android-demo 同形）
  native/server/                   # 测试服务器：monet-demo 加 [[bin]] server
```

- **测试服务器**：monet-conformance 的 HttpServer/echo 服务器目前 `127.0.0.1:0` 随机端口——加 `spawn_on(addr)` 固定端口 + `0.0.0.0` 绑定变体（LAN 真机可达），monet-demo 出一个 `server` bin 跑 HTTP 路由 + WS echo，打印端口。复用同一套服务器代码 = JS 电池与 Rust 电池打的是同一个靶
- **JS 电池项**（镜像 conformance 命名）：`http:get_plain_body` / `http:get_chunked_content`（断言观察到 ≥2 个独立 chunk）/ `http:post_reaches_server` / `http:redirect_followed` / `http:cancel_after_head_aborts_body` / `http:connection_refused` / `ws:round_trips` / `ws:close_code_fidelity` / `ws:ping_surfaced`
- 连接地址可配：模拟器默认 `http://localhost:<port>`，Android 模拟器 `10.0.2.2`，真机填 host 的 LAN IP
- TLS/pins 的 JS 级检查需要受信证书（自签 CA + SPKI hash 预置），标 v1.1 手动项（与 iOS SecTrust 链路验证同批，都用真机做）。**已实施（2026-11-09）**：`server` bin 默认带 TLS 对（https :8966 / wss :8967，fixture CA 内嵌；leaf SAN 覆盖 server 主机名的 `.local` mDNS 名——pin 键必须是主机名，monet-core 拒绝 IP 键）；leaf SPKI 每次启动现铸、经 `/leaf-spki` 下发（不烤进 App）。TLS 电池 6 项（trusted_ca / pin_match / pin_mismatch fail-closed / wss trusted / wss pin / wss pin_mismatch），iPhone 17 真机 6/6。两个电池侧的确定性前提记录在案：(a) TLS 电池服务器响应带 `Connection: close` 且不发 session ticket——URLSession 按连接池复用已握手的 TLS 连接（resumption 亦然），池内连接不再触发 server-trust challenge，pin 只在首个握手上评估；(b) 电池全局串行——hook/pins 注册表是进程级的，两电池并发互相污染。生产语义（URLSession 栈 pin 为 per-connection，见 urlsession plan）不受影响

## iOS 集成风险（M2 消解）

monet-urlsession 的 Swift shim 对象会进 superlib `librust_app.a`，最终链进 RN App（Obj-C++/Swift 混编）：

1. Swift 静态库链进非 Swift 主程序：iOS 12.2+ Swift runtime 系统自带，理论可行；需 podspec 补 `Foundation/Network` framework —— RN-RustTurbo.podspec 不是我们的，缓解：app Podfile `post_install` 补 OTHER_LDFLAGS，或 monet-rn 附带 podspec 文档说明
2. pod script phase 环境里跑 build.rs 的 swiftc（iphonesimulator SDK）——ios-demo 已验证 host 直接跑通，差异只在 env（DEVELOPER_DIR 等），预期无碍，M2 实测
3. cxx 生成头仅 Rust 内部消费，App 侧不 include → App 无需开 `-cxx-interoperability-mode`（预期，M2 实测链接符号即证）

## 实施里程碑

- **M0**：本计划入库（本文件）
- **M1**：packages/monet-rn 骨架（package.json + Cargo.toml + 空 Monet 模块 + vendor 同步脚本）+ rn-demo App 骨架（jsi-rs example 模式）
- **M2**：**首次三端构建全绿**（iOS 模拟器 + Android 模拟器）：Monet 模块带一个 `ping() -> String` 方法，App 调通——一次性消解聚合构建 × monet crates × Swift shim 进 superlib × Android 链接四重集成风险
- **M3**：fetch 通道全量（configure_pins/timeout/fetch/take_chunk/abort + pump + TS fetch/Response + server bin）+ JS 电池 HTTP 六项：iOS 模拟器、Android 模拟器双绿 + **iPhone 真机跑一次**（补上 transport 矩阵最后一块）
- **M4**：WS 通道（ws_* + TS WebSocket + 电池 WS 三项）双端绿
- **M5（v1.1）**：JS hook 回程协议 + TS hook API；TLS/pins 的真机手动验证项。**已实施（2026-11-08）**：observer/hook 双通道（见 docs/plans/2026-11-hooks.md 设计记录），电池 +8 项，双平台 18/18。**TLS/pins 真机验证已闭合（2026-11-09）**：iPhone 17 真机主电池 19/19 + TLS 电池 6/6（电池细节见上面"验证形态"节），顺带修掉两个真 bug——`ws_connect` 此前用 `PinConfig::default()` 建 WS transport（pins 到不了 WS 握手），iOS 的 `with_pins` 此前仅非空注册（`clearPins` 后 shim 残留旧 pins 继续生效）
- 每步：crates/ 内改动照常过 workspace 全套测试 + clippy/fmt；monet-rn crate 本身无 host 测试（jsi 依赖只在 app 环境解析），正确性由 rn-demo 电池承载——**这是与 workspace 内 crate 的验证策略差异，记录在案**

## 与 DESIGN.md 的对账

- "统一核心在 Rust、平台薄 transport"：落地为 vendor 的平台 cfg 依赖选择，App 二进制只编入对应平台栈
- 目标 #2（hooks）：v1.1（M5）
- 目标 #4（nitro 功能面）：fetch 兼容子集 + WS 先行；prefetch/token refresh 仍为 v2
- monet-dart（flutter_rust_bridge）：不受影响，core 不动
