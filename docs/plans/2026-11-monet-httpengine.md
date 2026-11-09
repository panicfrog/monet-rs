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

## H3 修订(2026-11-09,真机电池暴露)——pin 路径从未被覆盖,现状是未决缺陷

对账发现:H3 的"破坏性 pin 检查"**从未实现**——`run_httpengine_battery` 只挂明文
conformance 套件,`tls:pin_mismatch_rejects` 只挂 MonetRustlsTransport。pin 装配
(`addPublicKeyPins(host, Set<byte[]>, includeSubdomains, Instant)`)自 H3 起没有任何
检查,装配错误或引擎不强制都不会被发现。

真机(JS 电池,小米 API 36)实测暴露:**mismatch pin 放行 200**——`configurePins` 装
了错误 hash 后 fetch 正常完成,没有 `ERR_SSL_PINNED_KEY_NOT_IN_CERT_CHAIN`。
`https:trusted_ca_get` 通过(engine 本身 + NSC 锚正常),排除 engine 整体不可用;
wss(monet-ws)侧 pin mismatch 正确拒绝,排除 pin 配置面。

下一步(按序):① 模拟器(API 35 AOSP)复现 JS 电池同款 mismatch——若拒绝,则是该设备/
引擎模块不强制(MIUI 的 HttpEngine 主线模块版本差异,记录在案即结);若同样放行,
② 打日志查 `install_pins` 的 JNI 调用(异常被吞?HashSet 装配?);③ 无论根因,
`run_httpengine_battery` 补挂 TLS 形态的 pin mismatch 检查(LocalCa from_pkcs8 +
spawn_https_on),终结该路径无覆盖状态。

## H3 修订二(2026-11-09,步骤①执行完毕)——根因落定:local trust anchor 的 pin 豁免,已修已验

步骤①结果:API 35 google_apis 模拟器**同款复现**——TLS 电池 2/4,装配日志正常
(`addPublicKeyPins(localhost = 1 hash(es))` → `engine built (1 pin entries)`),
`https:pin_mismatch_rejects` 仍放行 200。排除 MIUI 模块差异,是平台语义(②的装配
怀疑同日由 android_logger 日志排除——装配链路从始至终没有错)。

根因(官方 API 文档落定):Chromium/Cronet 对 **local trust anchor**(用户安装 CA、
network-security-config 锚)默认**豁免 pin 强制**——`HttpEngine.Builder` 自带
`setEnablePublicKeyPinningBypassForLocalTrustAnchors(bool)`,默认 true。设计动机是
Chromium 安全 FAQ 的立场:pin 防的是公共 CA 误签发;对本地锚(SSL 调试代理、企业
MITM)强制 pin 会把调试/代理场景全部打死,官方措辞关掉豁免"highly discouraged"。
电池的 fixture CA 恰是 NSC `<base-config>` 锚——所以 `trusted_ca_get` 过(本地锚被
信任)而 pin 强制被跳过(本地锚豁免),两件事是同一个信任面的两面。

修复:`build_engine` 在 **pins 非空时**调
`setEnablePublicKeyPinningBypassForLocalTrustAnchors(false)`。取舍:显式配 pin 的 app
要的正是"本地装了 MITM CA 也不放行"——本地锚正是 pin 存在意义里的攻击面;不配 pin
的引擎保持平台默认(豁免),代理/调试场景不受影响。与 `addPublicKeyPins` 同为
API 34+,无版本分叉。

验证(修复后,双端):AOSP 模拟器 TLS 电池 2→3 pass,`https:pin_mismatch_rejects`
以真 pin 拒绝通过(检查断言错误消息含 pinning failed/PINNED,非泛化失败);小米真机
(API 36)同款 3 pass。剩余 3 失败全部是 wss 的已知边界(rustls-platform-verifier
读不到 NSC 锚,monet-rn plan 边界 1)——正是 M6 configureTrust 的立项依据。

步骤③的归宿:JS 电池的 `https:pin_mismatch_rejects` 就是暴露本缺陷的自动化覆盖,
修复后已真实强制,Rust 侧不缺这双眼睛。`run_httpengine_battery` 的封闭式 pin 检查
(修订一设想 LocalCa + spawn_https_on)需要引擎配自定义根,而 `HttpEngine.Builder`
的方法面**没有 custom roots API**(只有 pins/QUIC hints/DNS/cache/代理),本地铸的
CA 引擎一律 UnknownIssuer——即 M6 的 configureTrust(roots) 在 Android 只能落到
monet-ws/monet-rustls(<34 fallback),HttpEngine 侧是 no-op + 文档记录。M6 设计输入。
