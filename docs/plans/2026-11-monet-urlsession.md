# monet-urlsession：iOS 原生 transport（Swift shim + cxx）

## Context

Android 端已闭环（monet-rustls 默认 + monet-cronet 遗留 + monet-ws）。**iOS 必须原生**（用户明确定调：原生优先是总原则，monet-urlsession 是生产前置；纯 Rust 仅限开发/测试）——Apple 的 URLSession 由 Apple 永续维护，无版本天花板，且系统代理/cookie/钥匙串免费。

本 crate 是**独立 transport**，零 RN/JSI 依赖：开发与验证完全在 monet-rs workspace 内（macOS host 直跑 URLSession，iOS 交叉编译验证）。jsi-rs/monet-rn 是最后一步，本阶段不碰。

已定决策（DESIGN.md）：
- 不用 objc2；Swift 独占 URLSession delegate 状态机（nitro `NitroBufferedTaskDelegate` 为蓝本）
- 桥接 = Swift↔C++ interop + cxx（复用 cxx 生态，无第二桥接体系）
- 窄回调面：`transport_execute(req, {on_response_head, on_chunk, on_done/on_error})`
- HTTP = URLSession + delegate；WS = URLSessionWebSocketTask（同一个 shim）
- pinning：delegate challenge → 查 core（host 匹配语义在 Rust 一份）
- WebSocket/流式：head 先行、chunk 流式、CancelToken 语义与其他 transport 一致

## nitro-fetch 深读结论（已回填）

- **蓝本入口**：`HybridNitroFetchClient.requestStatic(req, bodyData, options, onResponseStarted, onData, onProgress) async throws -> NitroResponse`——静态方法、可脱离 JS 复用，我们的 TransportShim 镜像它
- **请求构建**：plain `URLRequest` + `addValue`；**上传全缓冲**（multipart/base64/string → `httpBody`，无 URLSessionUploadTask）——与我们 monet-cronet v1 的缓冲决策一致，v1 照抄
- **redirect**：`willPerformHTTPRedirection` delegate 返回 nil 阻断；`followRedirects` 默认 true；无 redirect/priority 时走无 delegate 快速路径
- **超时**：`req.timeoutMs > 0` → `r.timeoutInterval`（仅 per-request，无 session 级）——**我们要补：core 目前没有超时概念，Transport 层加默认超时配置**
- **优先级**：iOS 16+ `task.priority`（didCreateTask 挂钩）
- **cookie**：默认 `HTTPCookieStorage` 共享；`credentials == .omit` 时关——v1 跟随默认
- **session**：静态共享 `URLSessionConfiguration.default` + `useProtocolCachePolicy` + URLCache（32MB/100MB）
- **WS（意外发现）**：nitro iOS 主路径是 Network.framework `NWConnection` + **手写 WebSocket 帧**，`URLSessionWebSocketTask` 只是 Info.plist 可切的 fallback（带 challenge delegate pinning）——fallback 的存在证明 URLSessionWebSocketTask 路线可行且简单，我们直接用它
- **nitro 的 Swift↔非Swift 模式**：`@objc` bridge（`NitroWSPinningBridge` 等）供 Obj-C++ 调用——因为它的混合侧是 Obj-C++；我们的对应物是 cxx + interop
- **流式启发**（来自早前读的 NitroBufferedTaskDelegate）：`startsStreaming` Content-Type 启发（text/event-stream 等）、0.4s 缓冲窗口、256KB 文本阈值——v1 简化：一律流式（我们直接把 didReceive data 推给回调，缓冲策略留给前端）
- **冷启动**：`+load` → `UIApplicationDidFinishLaunchingNotification` → serial queue 上 prefetchOnStart；配置存 UserDefaults（加密）；token refresh 在 prefetch 前——将来 Rust bootstrap 的挂点即此处（本期不做）

## Swift↔C++ interop 研究结论（已回填，2026-11 本机验证）

- **双向机制确认可行**（本机 Swift 6.2.1 / Xcode 26.1.1 实测过 flag 与 typecheck）：
  - Swift → Rust：`swiftc -cxx-interoperability-mode=default` + modulemap（`module monet_urlsession { header "rust_bridge.h" export * }`，rust_bridge.h `#include` cxxbridge 生成头 + `rust/cxx.h`），经 `-I` 传入
  - Rust → Swift：trampoline.cpp `#include "Shim-Swift.h"`（`swiftc ... -emit-clang-header-path` 生成），调用 `@_expose(Cxx)` 公开 Swift 函数（生成为模块命名空间内的 C++ 内联函数）
  - 静态库：`swiftc -parse-as-library -emit-library -static -cxx-interoperability-mode=default`；iOS 交叉：`xcrun --sdk iphoneos swiftc -target arm64-apple-ios17.0 -sdk $(xcrun --sdk iphoneos --show-sdk-path) ...`
- **Swift 不能实现 C++ virtual interface**（6.x 仍不支持继承 C++ 类；`@cxx @implementation` 仍是实验性）→ **自由函数设计完全绕开它**，原风险 #1 的"Swift 实现 cxx 接口"问号消失
- 边界类型：原语/指针/切片可跨；`@_expose` 不支持 async/throws/generics——窄回调面恰好全是简单签名
- 陷阱：C++ **引用类型** import 会把最低部署抬到 iOS 16.4/macOS 13.3——边界只用自由函数+值类型规避；`.a` 消费者也要开 interop；生成头必须随 `.a` 分发
- **零生产先例**（Sourcegraph 无 cxxbridge+Swift-flag 代码）——该组合的先行者，M1 spike 价值高；Apple `swift-cmake-examples`（双向 Swift↔C++）是构建参考

## 定稿：回调链路形状

```
数据面（Swift → Rust，高频）：Swift 直接调 cxx 生成的 extern "Rust" 函数
    on_response_head(id, status) / on_header(id, name: &[u8], value: &[u8])
    / on_chunk(id, data: &[u8]) / on_done(id) / on_error(id, code, msg: &[u8])
    / on_redirect(id, location: &[u8])
    —— Swift 侧 BridgeGlue.swift 把 Data/String 转成 rust::Slice 再调用，丑细节隔离在这一层

控制面（Rust → Swift，低频）：cxx extern "C++" 声明 → trampoline.cpp → @_expose(Cxx) Swift 函数
    transport_start(id, url/method 指针+长度, follow_redirects, timeout_ms)
    transport_add_header / transport_set_body / transport_cancel
    ws_open / ws_send / ws_close
    —— trampoline 把 &[u8] 转 raw pointer 传 Swift（指针是 @_expose 可跨类型）

生命周期：Swift 持有 task + delegate（标准 delegate 模式，无跨边界对象持有）；
Rust 侧全局注册表 request_id(u64) → RequestState{oneshot, BodySender, cancel 接线}——
与 monet-cronet 的 client_context 方案同构，但 id 解引用代替裸指针，更简单
```

超时：core 无超时概念，在 ffi 面传 `timeout_ms`，Swift 侧落 `URLRequest.timeoutInterval`（nitro 模式）。

## 实施内容

```
crates/monet-urlsession/
├── Cargo.toml        cxx、cc（build）、monet-core；无 rustls（pin 查询走 core）
├── build.rs          编排：cxx 生成头 → modulemap → swiftc（.a + clang header）
│                     → cc 编译 trampoline.cpp → 链接；按 CARGO_CFG_TARGET_OS 分支 macOS/iOS
├── src/
│   ├── lib.rs        Transport + WebSocketTransport 实现 + PinConfig 注册
│   ├── ffi.rs        #[cxx::bridge]：数据面 extern "Rust" + 控制面 extern "C++"
│   ├── registry.rs   request_id → RequestState 全局表（Mutex<HashMap> + AtomicU64 计数）
│   └── pin.rs        challenge 查询：pins_for_host → 序列化 hashes 给 Swift
├── cpp/trampoline.cpp
├── swift/
│   ├── BridgeGlue.swift      rust::Slice/指针转换隔离层
│   ├── TransportShim.swift   requestStatic 蓝本：共享 session + delegate 状态机
│   │                         （didReceive response/data、redirect、challenge→pin 查询、
│   │                          buffered 上传 v1；不做 nitro 流式切换启发，一律直推）
│   └── WebSocketShim.swift   URLSessionWebSocketTask：receive 循环 → 数据面回调，
│                         send/close 经控制面，delegate 收 open/close 事件
└── tests/
    ├── http.rs   本地 hyper 服务器（复用 monet-rustls tests 的服务器模式）
    └── ws.rs     本地 tokio-tungstenite echo（复用 monet-ws tests 的模式）
```

## 验证与里程碑

- **M0（第一步）**：把本计划完整写入仓库 `docs/plans/2026-11-monet-urlsession.md` 并提交——计划随仓库走，可在任何设备上继续实施
- **M1 spike（crate 内）**：build.rs 全链路 + 最小 Swift ping（`@_expose` 函数回调进 extern "Rust" pong）+ Rust 测试断言往返——一次性消解构建编排/双向链路/头生成顺序三个问号
- **M2 HTTP**：TransportShim + registry + Transport 实现 + 本地服务器测试（head 先行/chunk 流式/POST/redirect/取消/错误映射；pin challenge 需自签证书，可 `#[ignore]` 手跑）
- **M3 WS**：WebSocketShim + WebSocketTransport + echo 测试（close code/reason 往返）
- **M4 iOS 交叉编译**：`cargo build -p monet-urlsession --target aarch64-apple-ios`（build.rs iPhone SDK 分支）；真机留到 monet-rn
- 每步 workspace 测试全绿 + clippy/fmt/doc 干净 + 单独 commit（英文无署名）

**全部完成（2026-11-03）**：M0-M4 依次入库；M4 交叉编译首跑即过（cxx → modulemap → swiftc(iPhone SDK, arm64-apple-ios17.0) → cc trampoline → link，`Finished dev profile in 17.85s`）。真机/模拟器验证与 RN 集成（monet-rn、jsi-turbo）按既定路线留待下一阶段。

## M2 实施记录（HTTP 部分，2026-11）

数据面落地与计划的差异：Swift 不直接构造 `rust::Slice`（C++ 类模板，跨 interop 构造无先例），改由 build.rs 在 modulemap 的 `rust_bridge.h` 里生成指针版 inline 包装（`monet_on_chunk(id, ptr, len)` 等），Swift 只碰裸指针——丑细节反而隔离得更干净，BridgeGlue.swift 只剩 Data/String → 指针+长度的转换。控制面顺序定为：`add_header`/`set_body` 先组装，`transport_start` 最后发送（Swift 侧组装 pending URLRequest 后 resume），无需额外的"组装完成"信号。

实施中核实的事实：

- **head 与首块数据同行**：hyper 对未知长度响应在首个 body frame 就绪前不冲刷，URLSession 的 `didReceive response` 也随首批字节到达——"head 先到、body 卡住"的场景无法用"延迟首块"构造，测试改用"大块先行 + 尾块延迟"；小间隔分块可能被 URLSession 合并成一个 data 事件（512KiB 强制独立交付）
- **head 后取消需要独立观察者**：core 无 runtime（monet-rustls 靠 tokio pump 任务全程盯 token），urlsession 在 head 交付时起一个停靠线程盯 CancelToken，终态事件 drop 掉 shutdown sender 让线程退出；monet-cronet 目前只有 head 前取消 + body-drop 检测，此处对齐的是 rustls 语义
- **无 header/body 的请求也必须能启动**：Swift 侧 start 用 get-or-create 拿请求盒（最初 guard 住"必须先有组装"导致裸 GET 静默不发）
- **测试自带超时**：`wait_for(step, fut)` 辅助函数（futures_timer，10s），挂住会变成带步骤名的明确失败——排查本轮问题全靠它定位

M2 pinning 部分随后落地：Rust 侧 `pin.rs` 全局注册 PinConfig（`UrlsessionTransport::with_pins`；shim 是进程级单例，注册随之全局，last-wins），数据面新增 `pins_for_host(host, out) -> usize`（Swift 提供 64×32 字节缓冲，0 = 未钉、走系统默认处理）；Swift 侧 `PinningShim.swift` 只做机械部分——异步 SecTrustEvaluate + 从 DER 证书直接切出 SubjectPublicKeyInfo 做 SHA-256 比对（nitro `CertificatePinning.swift` 同款），拒绝的 challenge 会以 -999 完成任务，didCompleteWithError 据此重映射为证书错误而非 Cancelled。host 匹配语义（walk-up/includeSubdomains/过期）只在 monet-core 一份，与 Rust transports 完全一致。Rust 侧查询服务有单元测试；Swift 侧 SecTrust 链路的集成验证需要受信证书，按计划留待真机阶段（monet-rn）。

## M3 修订：WebSocket 改走 NWConnection + 手写帧（对齐 nitro，2026-11）

原计划用 `URLSessionWebSocketTask`（nitro 的 Info.plist fallback）。用户定调改为对齐 nitro 主路径：**Network.framework `NWConnection` + 手写 RFC 6455 帧**。动机（nitro `NWWebSocketConnection.hpp` 注释原文为证）：URLSessionWebSocketTask 会把 1012/1013/1014 映射成 1005 并丢掉 reason，原始 close 帧不可达——monet-core 的 `CloseFrame` 契约要求 code/reason 精确往返（一致性测试赌注所在），fallback 路线直接违背这一点。NWConnection 仍是 Apple 原生栈（Network.framework），"原生优先"原则不变，只是从"系统做协议"换成"系统做传输、我们做协议"。

蓝本：`packages/react-native-nitro-websockets/ios/NWStreamWebSocketConnection.mm`（~1400 行 Obj-C++）。移植到 Swift 的范围与裁剪：

**移植（v1 全量）**：手写 Upgrade 握手（Sec-WebSocket-Key + SHA1/GUID 校验 Accept、协议回显校验、严格 101/Connection/Upgrade 校验）；帧解析状态机（分片、控制帧规则、服务端帧不得带 mask、16MB 上限、text 消息 UTF-8 校验）；掩码写帧（7/16/64 位长度）；自动 pong；完整关闭握手（双向、5s 关闭超时、60s 握手超时、1006 不洁关闭语义、code 合法域校验）；wss 走 `sec_protocol_options_set_verify_block` 注入 pinning（复用 PinningShim，verify block 异步完成兼容）。

**裁剪（v1 不做，记录在案）**：permessage-deflate（不主动 offer；服务器强行协商 → 1002 拒绝，符合 RFC；URLSessionWebSocketTask 同样不支持，无回退损失）；redirect 跟随（3xx 视为失败；monet-ws/tokio-tungstenite 同样不跟随）；cookie/UA 注入（自定义 headers 支持，系统 cookie 留待前端层）。

**桥接面（沿用 M2 模式）**：控制面 `ws_add_header`/`ws_open`/`ws_send`/`ws_send_ping`/`ws_send_pong`/`ws_close`/`ws_cancel`（trampoline → `@_expose`）；数据面 `on_ws_open`/`on_ws_message(id, is_binary, data)`/`on_ws_ping`/`on_ws_pong`/`on_ws_close(id, code, reason, was_clean, error)`（错误切片空 = 无错误；open 前 failure 用它解析 connect future）。Rust 侧 `src/ws.rs`：WS 注册表 + 每连接一个停靠 pump 线程消费 user_to_network（无 runtime，同 M2 watcher 模式；终态事件 drop shutdown sender 回收——pump 内是 poll_fn 三路轮询，oneshot 只能 await 一次）。

M3 已落地（2026-11，本地 tokio-tungstenite echo 服务器 7 项测试全绿）：text/binary 往返、子协议头到达、**服务端 1012+reason 精确往返**（NWConnection 路线的决定性验证，URLSessionWebSocketTask 在此会报 1005 且丢 reason）、客户端 close(4321) 代码/reason 到达服务端、ping 同时呈现给用户且被自动 pong、token 预取消 fail-fast、连接拒绝映射 Transport 错误。handshake/关闭均为 60s/5s 双超时（世代计数防陈旧定时器）。
