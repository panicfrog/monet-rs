# monet-httpengine:Android 平台 HttpEngine 轨道(API 34+)

## Context

Android 14(API 34)起平台自带 `android.net.http.HttpEngine`——Cronet Java API 的系统化封装(事实核实见 DESIGN.md 2026-11-04 条目:模拟器实测引擎 121.0.6167.71,原生 pinning/QUIC hints/连接迁移/Brotli 齐全)。官方 Cronet **二进制分发**死了不假,但 API 34+ 的**平台本身**把引擎带了回来——原生优先在 Android 复活,且零 App 体积、无 GMS 工件依赖。

轨道定位:`monet-httpengine`(JNI → 平台 API,API 34+)+ `monet-rustls`(<34 与国产无 GMS ROM 兜底)。`Transport` trait 保证是配置选择;WS 不在此轨(Cronet 无 WS,monet-ws 继续承担)。

## 关键设计决策(已定)

### Kotlin 机械桥 + Rust 状态机

Rust 无法直接实现 Java 接口(`UrlRequest$Callback`/`UploadDataProvider`/`Executor`),需要一个宿主侧 Kotlin 桥——与 rustls-platform-verifier 的 CertificateVerifier AAR 同款模式,但方向相反:**Kotlin 只做机械转发**(回调 → native 方法,带 request id),状态机/注册表/取消映射全在 Rust(与 monet-urlsession 的"Swift shim 机械、Rust 做语义"同构)。

桥的三件套(demo 放 `com.monet.demo.httpengine`,monet-rn 将来放自己的包):
- `MonetRequestCallback(id) : UrlRequest.Callback` → 六个回调全部转 `external fun on*(id, request, info, extra)`
- `MonetUploadProvider(bytes) : UploadDataProvider` — v1 缓冲上传(android.net.http 没有 Chromium 的 `UploadDataProviders` 助手类,自己实现)
- `MonetDirectExecutor : Executor` — 直接执行 + `setDirectExecutorAllowed(true)`,回调进 Cronet 内部线程 → Rust 侧 attach

符号解析:v1 用 JNI 名字改编直绑 demo 包;monet-rn 集成时改 **RegisterNatives**(`init(vm, context, bridgeClass)` 传类名字符串),crate 不再绑死宿主包名。

### 生命周期与初始化

`HttpEngine` 构造需要 `Context`,回调需要 JVM——`httpengine::init(&JavaVM, Global<JObject> context)` 先行(与 rustls-platform-verifier 的 Android 初始化同款),内部:
- 存全局 JavaVM + Context global ref + app ClassLoader(找桥类用;`android.net.http.*` 在 boot classpath,find_class 即可)
- `HttpEngine.Builder(context)`:遍历 `PinConfig` → `addPublicKeyPins(host, Set<byte[]>, includeSubdomains, Instant)`(原生 SPKI pinning!)+ `setEnableBrotli(true)` → `build()` 存全局 ref
- `SDK_INT >= 34` 检查,不满足直接报 transport 错误(兜底切换由宿主决定)

### 请求状态机(移植 monet-cronet 的形状)

registry:`id → RequestState{head_tx, headers, body_tx, request: Global<UrlRequest>}`(Mutex<HashMap>);回调线程 attach 后进 Rust:
- `onResponseStarted` → 状态码 + `HeaderBlock.getAsList()` 遍历入 HeaderMap → head 交付(oneshot)→ `request.read(NewDirectByteBuffer(64KB))`
- `onReadCompleted` → 直 буфер 地址 + remaining 拷贝 → `send_chunk`;失败(消费端没了)→ `request.cancel()`;继续 read
- `onRedirectReceived` → v1 恒 `followRedirect()`(与 monet-cronet v1 对齐)
- `onSucceeded/onFailed/onCanceled` → 终态:complete/abort(Cancelled)/移除
- execute 侧:race head_rx vs CancelToken(与 monet-urlsession 同款),cancel 分支 `request.cancel()` + 移除

### v1 已知裁剪(记录在案)

- **无 per-request 超时**:`UrlRequest.Builder` 面上没有 timeout 项(C API 的 `timeout_ms` 在平台 API 里缺失)——超时继续由 rustls/iOS 承担,此轨记录分歧
- 上传全缓冲(UploadDataProvider 实现为 byte[] 转发;流式上传留后续)
- redirect 恒跟随(策略开关留 core 层统一做)
- `getNegotiatedProtocol()`(h2/h3 可见性)暂不外露,留给将来 telemetry

## 验证形态

monet-demo 增加 `run_httpengine_battery()`(cfg android,SDK≥34 门槛):**整个 monet-conformance HTTP 套件**打在 HttpEngineTransport 上——与 rustls/urlsession/cronet 同一靶。模拟器(Android 15)先行,国产无 GMS 真机验证留待设备阶段(与 platform-verifier 同批)。

## 里程碑

- **H1**:crate 骨架(host stub + android cfg)+ Kotlin 桥 + init/JNI 地基 + 引擎版本探针入电池
- **H2**:Transport 全量(head/body/cancel/error)+ conformance HTTP 套件模拟器全绿
- **H3**:pins 原生注入(破坏性 pin 检查:错 hash → 引擎拒绝)+ 提交
