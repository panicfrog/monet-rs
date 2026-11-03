# monet-rs 设计文档

基于 Rust 的跨端网络请求库，参考 [react-native-nitro-fetch](https://github.com/margelo/react-native-nitro-fetch)（Margelo，基于 Nitro Modules）的思路，用 [jsi-rs](https://github.com/laptou/jsi-rs)（本地 `/Volumes/YYP_PSSD/Source/jsi-rs`）的 TurboModule 框架承载 React Native 集成，核心保持可扩展到 Flutter/Dart（flutter_rust_bridge）。

## 目标

1. **首要**：Rust 直接调用原生网络栈 —— Android 用 Cronet（C API FFI），iOS 用 URLSession（Swift shim；桥接方案见下）
2. **网络请求 hooks**：请求/响应拦截链，Rust 原生 hook + JS 侧 hook 走同一条链
3. **多前端复用**：核心为纯 Rust，无 RN/JSI 依赖；先接 RN（jsi-turbo），后续接 Dart（flutter_rust_bridge）
4. 对标 nitro-fetch 的功能面：WHATWG fetch 兼容 API、prefetch（内存 + 磁盘缓存 + 冷启动预取）、token refresh、证书固定、WebSocket（见专门小节）

## 参考架构（nitro-fetch 的分层，供对照）

```
JS 层 (TS)          fetch-core, Response/Headers WHATWG 兼容封装
契约定义 (*.nitro.ts)  HybridObject 接口 = 单一事实来源
nitrogen 生成层       shared/c++ 抽象接口 + Swift↔C++ Bridge + JNI 注册（纯桥接，无逻辑）
平台实现             iOS Swift → URLSession；Android Kotlin → Cronet（两份平行实现）
JSI 集成             由 react-native-nitro-modules 提供
```

关键认识：nitro 的"统一 API"在 TypeScript 契约层，C++ 只是生成的胶水；平台实现互不共享代码。
monet-rs 的差异：**统一核心放在 Rust**，平台只保留薄薄的原生 transport，hook/缓存/预取逻辑全平台共享一份。

## monet-rs crate 分层

沿用 jsi-rs 中 `jsi` 与 `jsi-rn` 分离的哲学——核心不得依赖任何前端运行时。

```
monet-core            纯 Rust，零 RN/JSI 依赖
                      ├─ Request/Response 类型（bytes::Bytes）
                      ├─ Hook 链（RequestHook / ResponseHook）
                      ├─ prefetch 缓存（内存 + 磁盘）、token refresh
                      ├─ trait Transport（异步、流式 body）
                      └─ trait WebSocketTransport + pin 配置解析/host 匹配
                            ▲
      ┌─────────────────────┼─────────────────────┐
monet-cronet         monet-urlsession        monet-rn
Android Transport     iOS Transport           jsi-turbo #[RustTurboModule] 包装
(FFI to Cronet C API) (Swift shim，见下)      WHATWG 兼容 + JsScheduler 回 JS 线程
+ monet-ws (纯Rust WS) + URLSessionWebSocketTask
                                              (将来) monet-dart
                                              flutter_rust_bridge 包装同一个 core
```

Android 的 WS（monet-ws，tokio-tungstenite + rustls）也是全平台兜底/桌面实现；hyper+rustls 兜底 HTTP transport 同理放独立 crate。

### Transport trait（已实现于 monet-core）

```rust
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub trait Transport: Send + Sync {
    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>>;
}

pub trait WebSocketTransport: Send + Sync {
    fn connect(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<WebSocketStream>>;
}
```

- `Request`/`Response` 按值传递（Tower 式），hook 同理（`on_request(Request) -> BoxFuture<Result<Request>>`）——引用无法跨 await 进入 `'static` boxed future，所有权让 hook 可组合
- `CancelToken`：自研 waker 位取消，对应 Cronet `request.cancel()` / URLSession `task.cancel()` / rustls 关流

- `Response` 持有流式 body（`Bytes` chunk 流抽象），非一次性缓冲
- core 层异步用 boxed future + channel 表达，不绑定具体 runtime；各 wrapper 自行适配（RN 走 jsi-turbo 的 executor——futures_lite `block_on` 固定 worker 池，非 reactor；Dart 由 flutter_rust_bridge 接管）

### Hook 系统（差异化核心）

```rust
trait RequestHook  { async fn on_request(&self, req: &mut Request) -> Result<()>; }
trait ResponseHook { async fn on_response(&self, res: &mut Response) -> Result<()>; }
```

- Rust 原生 hook：编译进核心，零 FFI 开销（改写 URL、注入头、改写响应体）
- JS 侧 hook：monet-rn 层把注册的 JS 回调（经 CallInvoker marshal 回 JS 线程）适配成同一 trait，与 Rust hook 共享一条链
- 同一 hook 逻辑对 RN 和将来的 Dart 生效

### 实施路径：原生 transport 优先

`Transport` 的**原生实现是首要目标**：Android 走 Cronet C API FFI，iOS 走 Swift shim 调 URLSession。`monet-core` 与原生 transport 并行开发，`Transport` trait 的接口设计直接以原生 API 的能力为约束（回调式、流式 body、取消语义）。

纯 Rust 实现（hyper + rustls + quinn）是**备选/兜底 transport**：用于开发期 core 层的单测基线、不支持原生栈的环境（如桌面/服务端测试），以及原生绑定受阻时的临时替代。它不是架构前提，不得反向侵蚀 core 的接口设计——core 面向 trait 编程，不为 hyper 的便利性妥协。

## 可行性核实结论（2026-11 调研）

| 平台 | 路径 | 结论 |
|---|---|---|
| Android / Cronet | 官方二进制发行版自带 C API（`cronet_c.h` + `libcronet.so`），为 FFI 消费者设计；bindgen 生成 Rust 声明。**已核实**（cronet-embedded 143.7445.0 AAR 符号表）：C API 有完整 pinning（`Cronet_PublicKeyPins_*` + `EngineParams_public_key_pins_add` + local-trust-anchor bypass 开关），共 259 个 `Cronet_*` 符号；**Java API 与 C API 均无 WebSocket**（Java `newWebSocket` 已从上游移除）。nitro-fetch 用的是 Java API（Kotlin 调 `CronetEngine.Builder`），其 WebSocket 另起炉灶（见下）。我们维持 **C API 直连 Rust**：pinning 齐全、无 JNI hop、回调在 native 线程直接进 Rust；Java API 相对优势（RequestFinishedInfo/telemetry）不值得多一层 Kotlin+JNI。⚠ **上游风险（2026-11 发现）：Cronet C API 已从 Chromium 146 起被上游移除**（146+ 发行包不再含 `include/`，源码树 `components/cronet/native/` 已删除）。monet-cronet-sys 钉 143.7445.0（头文件 143→145 逐字节相同），升级天花板 145.0.7569.0；再往后的迁移路径：SagerNet fork（仍携带 C API）或改走 JNI 调 Java API | ✅ 直接可行；无广泛维护的现成 sys crate（crates.io 有小众 `cronet` crate），需自维护薄绑定。版本需 ≥143.7445.0（AGP9 namespace 检查，nitro build.gradle 注释） |
| iOS / URLSession | **不使用 objc2**（决策 2026-11：delegate 状态机、autoreleasepool、生命周期管理用 Rust 重写 ObjC delegate 的代价过高；nitro-fetch 的对应实现是纯 Swift，已验证）。采用 Swift shim：Swift 独占 URLSession delegate 状态机，暴露窄回调面（`transport_execute(req, {on_response_head, on_chunk, on_done/on_error})`）。**桥接已决策：Swift↔C++ interop + cxx**（jsi-rs 的 RN 集成本就是 cxx，复用同一边界与构建机制，零新增桥接技术；swift-bridge 已评估未采用——0.1.x 无稳定承诺，且会在同一二进制引入第二套桥接体系） | ✅ 可行；delegate 回调 → Swift async 已由 nitro 的 `NitroBufferedTaskDelegate` 验证，我们只需把出口从 JSI 换成桥接回调 |

## Android transport 双轨决策（2026-11，基于目标市场含中国大陆）

目标 App 市场含国内（大量无 GMS 设备），据此定**双轨**，由 `Transport` trait 保证两轨是配置选择而非架构分叉：

| 轨道 | 状态 | 适用 |
|---|---|---|
| **monet-cronet（默认）**：C API 直连 + embedded `.so`（143.7445.0 AAR） | ✅ 已实现 | 任何设备可跑（含国内）；版本自控，缺点是每 ABI 4-7MB 且钉在 ≤145 |
| **monet-cronet-jni（后续增强）**：Java API + provider 模型 | 未开工 | GMS 设备走 `play-services-cronet`（零 `.so`、版本由 Google 经 GMS Dynamite 模块更新，绕开 145 天花板）；非 GMS 设备仍需 embedded `.so` 兜底 |

决策依据的核实事实（2026-11）：
- 官方 Cronet 二进制分发已实质停止：Google Maven 上 143.7445.0 是最后一个真 AAR（500.x 全是空壳，无 `.so`）；GCS Release 包停在 145.0.7569.0（146 起整个目录消失）；chromium/cronet GitHub 无 releases。**Java API 与 C API 的二进制停在同一个地方**——切换 JNI 并不解决天花板，天花板是分发问题
- `play-services-cronet` 18.1.1 是唯一仍被 Google 持续更新的 Cronet 二进制渠道：AAR 零原生代码，纯 Java 适配层（`PlayServicesCronetProvider`/`CronetProviderInstaller`），真实现住在 GMS 的 **Dynamite 模块**里，设备级共享、随 GMS 更新——但仅 GMS 设备可用，且 Cronet 是动态模块（首用可能触发下载），线上版本碎片化
- 结论：国内市场必须保 embedded `.so` 轨道；GMS 轨道作为海外增强，不阻塞 v1



**事实**：Cronet（Java API 和 C API）现在都**没有** WebSocket——旧的 `ExperimentalCronetEngine.newWebSocket` 已从上游移除（2026-11 从 143.7445.0 的 api.txt 与 `.so` 符号表核实）。nitro-fetch 因此把 WS 做成独立伴生包 `react-native-nitro-websockets`：C++ libwebsockets + mbedTLS（vendored 源码，Android/iOS 同一份），TLS 用构建时内嵌的 Mozilla CA bundle，pin 校验在握手完成后、headers 发出前（为拿完整 peer chain 甚至 include 了 LWS 私有头）。

**monet-rs 方案**（与 HTTP transport 同一哲学：原生优先，纯 Rust 兜底）：core 定义第二个 trait `WebSocketTransport`。

| 平台 | WS 实现 | 说明 |
|---|---|---|
| iOS | **URLSessionWebSocketTask**（原生） | 落在 monet-urlsession 同一个 Swift shim 里：同一个 session delegate、同一个 pinning 注入点、系统信任库免费；headers 经 URLRequest 支持。生产构建可完全不链 rustls |
| Android | **纯 Rust（tokio-tungstenite + rustls）** | Cronet 无 WS，且不为此引入 Kotlin 层。pin 校验用 rustls 自定义 `ServerCertVerifier`，与 HTTP 共享 core 同一套 pin 逻辑 |
| 兜底/桌面/测试 | 同一份 tokio-tungstenite + rustls | 与 hyper+rustls 兜底 transport 角色完全对称 |

代价：WS 两份实现（iOS 原生 + Rust），行为可能有细微差异——缓解方式与 HTTP transport 相同：core 针对 trait 写一致性测试套件，两个实现跑同一套（这也是整个架构的核心赌注）。待验证：`URLSessionWebSocketTask` 的 permessage-deflate 支持情况（历史上长期缺失，tokio-tungstenite 支持）。

**已知坑**：Android 上 rustls 读系统信任库（`/system/etc/security/cacerts` + conscrypt APEX）与 `network_security_config.xml` 的兼容性需要验证；兜底方案是内嵌 Mozilla CA bundle（webpki-roots，即 nitro 的同款做法），后续再补系统 CA 支持。

## 证书固定（pinning）架构

nitro 的教训：pin 配置解析 + host 匹配逻辑在它那边复制了三份（`CertificatePinning.kt`、`NitroWSPinning.kt`——注释自承 "keep the two in sync"、iOS `CertificatePinning.swift`）。

monet-rs 改为**一份**：解析与 host 匹配（walk-up + includeSubdomains + 过期）放 monet-core，配置文件沿用 nitro 的 `nitro-fetch-pinning.json` 格式（SPKI SHA-256，fail-closed——配置坏了拒绝请求而非静默关闭）。各 transport 只接收结构化的 pin 集合并注入：

- Cronet：`Cronet_EngineParams.public_key_pins` + `enable_public_key_pinning_bypass_for_local_trust_anchors = false`（Cronet 用自带 BoringSSL，无视 network_security_config；用户装的用户 CA/代理证书不得绕过 pins）
- rustls（WS + 纯 Rust HTTP 兜底）：自定义 `ServerCertVerifier`，先标准链校验再对 verified chain 做 SPKI 匹配（nitro 的 `PinningTrustManager` 同款语义）
- iOS URLSession：**注意 pinning 不是"再校验一次证书"**——标准校验（链/有效期/主机名/系统信任库）URLSession 默认已做，免费。Pinning 是其上的额外策略（链合法还须含钉住的公钥），而 URLSession 没有表达它的配置项，唯一注入点是 session delegate 的 server-trust challenge。落点就在 Swift shim 已有的 delegate 里，不新增机制（参考 nitro `NitroURLSession.swift` 的双 delegate 结构、`CertificatePinning.swift`）。匹配语义留在 Rust：Swift 收到 challenge 时向 core 查询"该 host 是否被钉、hashes 为何"（walk-up/includeSubdomains/过期匹配只此一份），Swift 只做机械的 `SecTrustEvaluate` + SHA256 比对



1. **iOS Swift↔C++ interop 集成**（已定路径，待验证执行细节）— Swift shim 经 C++ interop 挂到既有 cxx 边界上；需要验证：Swift 能否实现 cxx 生成的 virtual interface、delegate 生命周期跨边界的持有、RN pod 开 `-cxx-interoperability-mode` 的构建影响
2. **Cronet sys crate** — bindgen + 预编译二进制管理（AAR 内 `jni/<abi>/libcronet.<ver>.so`，每 ABI 4-7MB；Rust 侧 dlopen 或 gradle 分发）+ 回调线程 → Rust 异步的 marshal
3. **线程模型** — Cronet/URLSession 回调线程 → core 异步 → JS 线程（CallInvoker）
4. **流式 body** — core 用 `Bytes` chunk 流；RN 包成 JSI stream，Dart 包成 `Stream`
5. **冷启动 prefetch** — 需要 JS 之前的原生入口初始化 Rust 库（Android ContentProvider / Kotlin bootstrap 调导出的 Rust fn；iOS 类似）。参考 nitro 的 `NitroBootstrap.mm` / `NitroAutoPrefetcher`
6. **Android 系统信任库** — Cronet 自带 BoringSSL 无视 network_security_config（pins 已由 EngineParams 覆盖）；Android 纯 Rust WS（rustls）读系统 CA + 用户装 CA 的路径需验证，兜底 webpki-roots 内嵌
7. **WS 双实现行为一致性** — iOS 原生 URLSessionWebSocketTask vs Rust tokio-tungstenite：close code 语义、permessage-deflate（iOS 支持情况待核实）、ping/pong 策略；以 core 层 trait 一致性测试套件兜住

## 对比 nitro-fetch 的得失

**得**：一份 Rust 核心；hooks/缓存/token-refresh 全平台共享；Flutter 路径清晰；无需 nitrogen 式代码生成（jsi-turbo 已有 TS 绑定生成）。

**失 / 需补**：
- react-native-worklets 集成（nitro 生态特有，后续项）
- 系统集成需自己做：系统 cookie（NSHTTPCookieStorage 与 WebView 共享）、系统代理、钥匙串
- Cronet 二进制体积（每 ABI 几 MB）
- 冷启动 prefetch 的原生入口 plumbing

## 参考资料

- nitro-fetch 本地检出的源码：`/Volumes/YYP_PSSD/OpenSource/react-native-nitro-fetch`
  - iOS 实现：`packages/react-native-nitro-fetch/ios/`（HybridNitroFetch.swift、NitroURLSession.swift、NitroAutoPrefetcher.swift 等）
  - Android 实现：`packages/react-native-nitro-fetch/android/src/main/java/com/margelo/nitro/nitrofetch/`
  - 契约定义：`packages/react-native-nitro-fetch/src/*.nitro.ts` + `nitro.json`
  - WebSocket 伴生包：`packages/react-native-nitro-websockets/`（C++ libwebsockets + mbedTLS，`android/src/main/cpp/LwsPeerChain.c` 等）
- jsi-rs：`/Volumes/YYP_PSSD/Source/jsi-rs`（jsi / jsi-rn / jsi-turbo 分层，`jsi-turbo/docs/usage.md`）
- [chromium/cronet](https://github.com/chromium/cronet) — C API 文档
- objc2、swift-bridge 均已评估并排除（2026-11）：理由见"可行性核实结论"。最终采用 Swift↔C++ interop + cxx
- [flutter_rust_bridge](https://github.com/fzyzcjy/flutter_rust_bridge) — 将来的 Dart 前端
