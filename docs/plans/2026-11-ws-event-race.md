# ws 事件投递竞态——调查记录（2026-11-13，**已解决**）

## 0. 结论（同日，接手后）

**根因不在 native / jsi-rs，在 `ts/websocket.ts` 的 GC 根设计。** 连接建立后
socket 只剩 `liveSockets` 里的 WeakRef（"用户作用域持有在用的 socket"）。
但 `await nextMessage(ws)` 这种写法里，用户作用域就是挂起的 async 帧，
它只经由 ws.onmessage 持有的 resolve 被引用——与 `connecting` 注释里
握手期那个"无根环"完全同构，只是发生在 open 之后。一次 young-gen 回收
落在这个窗口 → WeakRef 被清 → `on_message` 到达 JS 时 `resolveLive`
返回 null → 信号被静默吞掉，wire 一切正常。blob 检查只是把分配节奏
推到了这个窗口上；§5 矩阵里"任何 JS 扰动都能挪走病灶"正是 GC 时机
敏感的表现。

**确证**（iOS，2/2）：在 dispatcher 的 null 分支打点 →
`__diag_message_null&status=3&detail=true`——on_message 送达了 JS，
`liveSockets.has(id)` 为 true，但 `deref()` 为 undefined。

**修复**：`listenedSockets` 强引用表——已建立（OPEN/CLOSING）且挂着
待消费者（`onmessage`/`onclose`/`onreconnect` 任一非空）的 socket 强持有
（即 WHATWG"有事件监听的 open socket 不得回收"规则）；否则只留弱引用。
`onopen`/`onerror` 不算——`ws:dropped_socket_collectable` 正是带着这两个
handler 丢弃 socket 的，释放契约保持不变。

**验证**：iOS 34·0 ×4（含 dropped_socket_collectable）；Android ws 检查
7/7 全绿。

**顺带发现**：
- 工作树 App.tsx 里 `http:retry_policy_succeeds` 被 blob 检查**覆盖掉了**
  （编辑事故）——这就是接手时 iOS 只有 33 项且复现不出来的原因。已恢复到
  原位置（blob 检查之前），恢复后 34 项确定性复现 2/2，修复后 4/4 绿。
- **另一个未解问题（与本病无关）**：Android 7 轮里有 3 轮各有一个
  **流式上传**检查超时——`formdata_blob_part`、`formdata_content_uri`、
  `streaming_upload`（W2 起的老检查）各一次，全部走 `fetchStream` 路径，
  无 ws 参与，iOS 未见。是否为预先存在（HEAD 上也有）尚未确认。

以下为原调查记录（§6 的假设 1/3 均不成立，留作历史）。

> **给接手者的状态**：病灶已定位到必要触发条件，根因未明。本文档自足：
> 复现步骤（确定性）、wire 层证据、完整排除矩阵、存活假设、仪器清单。
> 阅读顺序：症状 → 复现 → 证据 → 矩阵 → 下一步。

## 1. 症状

JS 电池的 ws 检查（通常是 `ws:round_trips`）超时（10s/15s），native 侧既不
触发 `on_open` 也不触发 `on_error`，JS 永远等在 `waitForOpen`/`nextMessage`。
**病会在不同 ws 检查之间游走**（round_trips / reconnect_after_abrupt_drop /
ping_surfaced 都当过受害者），取决于 JS 时序——不是特定检查的问题。

两个平台都复现（iOS 模拟器 6/6 确定性失败；Android 同日 5/6 失败），与
"monet-rn 的 jsi-turbo 层是共享代码"一致。ws 代码本身在触发补丁中一行未动
（W5 交付时 Android 5 连绿 + iOS 3 轮绿）。

## 2. 触发条件（已确认的必要条件）

**一个 Blob 作为 fetch body 走流式上传**——即 `fetch(url, {body: blob})`，
经过 `packages/monet-rn/ts/blob.ts` 的 `blobBodyStream` +
`ts/fetch.ts` 的 `isBlob` 分支。这是 v2 补丁（formdata blob part）引入的
新路径。RN 全家桶电池里只有这条路径会这样用。

关键事实：**这个竞态是潜伏的、预先存在的**——blob body 只是踩中它的时序。
修法大概率在 monet-rn 的 jsi 层（或 jsi-rs），不在 blob 路径本身。

## 3. 确定性复现（30 秒/轮）

环境（2026-11-13 时）：
- 电池服务器：`cargo run -p monet-demo --bin server`（8964/8965 明文 +
  8966/8967 TLS），报告 println 到 `/private/tmp/battery-server.log`
  （用 `stat -f%z` 打 mark，只认增量——见设备陷阱 memory 的读数通道纪律）
- iPhone 17 模拟器：`711B0245-BB59-4C09-A2A5-C8CEDD563D79`，bundle
  `com.monet.rndemo`（`za_web_sim` 模拟器是另一个项目的，别动）
- Metro 在 8081（改 JS 即时生效，无需重装）

```
SIM=711B0245-BB59-4C09-A2A5-C8CEDD563D79
xcrun simctl terminate $SIM com.monet.rndemo
sleep 2
MARK=$(stat -f%z /private/tmp/battery-server.log)
xcrun simctl launch $SIM com.monet.rndemo
sleep 32
tail -c +$((MARK+1)) /private/tmp/battery-server.log | grep REPORT
```

当前工作树（见 §8）状态即复现态：**iOS 34 项检查，blob 检查在位 →
ws 挂（6/6）；把 blob 检查禁掉/加任何额外 JS → 绿**。两种改法各做过多次，
heisenbug 特性稳定。

## 4. wire 层证据（TCP 逐字节代理 + NW 日志）

字节级 TCP 代理（见 §7）+ `log stream` 的 Network 事件抓包，两个决定性观察：

1. **直连失败的轮次**：客户端发出 175B 升级请求（纯握手），服务器回 141B
   （`HTTP/1.1 101 Switching Protocols` + 三个头，正好这个长度），然后
   ~15-20ms 后连接死。**hello 从未发出——`on_open` 没到 JS。** 服务器侧的
   FIN 是次生症状（客户端 connect 任务卡死后的清理），别去追服务器。
2. **经代理的一轮**：101 → 客户端发出 masked `hello-ws`（14B）→ 服务器
   echo 回到客户端 socket（10B）→ **之后客户端一片死寂**：没有二进制帧、
   没有 close 帧、10s 超时。**`on_message` 没到 JS**，但 wire 上 echo 已
   送达。

即：受害事件不固定（open 或 message），但都是 **native→JS 的回调投递**，
且 wire 层一切正常。

辅助证据：
- 服务器 30 连发顺序探测全部 0-9ms（npm `ws` 库），裸 TCP 最小握手也健康
- valve log（泵生命周期，986 行）显示 blob 检查的五个 fetch 泵全部正常完成
- 无 panic（模拟器系统日志 grep 无果）
- 失败轮的 10s 超时回调本身正常触发且 report 正常发出（fetch/HTTP 通道
  同一时刻完全健康——**只有 ws 的 TurboCallback 通道死**）

## 5. 排除矩阵（全部做过，各至少一轮）

| 配置 | ws 结果 |
|---|---|
| 完整 blob 检查（Blob 构造 + FileReader + blob body fetch ×2 + form blob ×2） | **挂 6/6** |
| FileReader 绕过（readBlob 返回 `Promise.resolve(new ArrayBuffer(0))`，检查在第一个断言中止 → 只跑了 Blob 构造 + 一个 0 字节 blob body fetch） | **挂**（病挪到 reconnect） |
| 只构造 Blob（×3，无 fetch 无 FileReader） | 绿 |
| Blob 构造 + **字符串体**流式 fetch（generator 内有 promise 挂起） | 绿 |
| 200MB 纯垃圾分配（GC 压力） | 绿 |
| RN 全局 fetch（原生模块异步机制，非 monet） | 绿 |
| blob 检查末尾显式 `blob.close()`（确定性释放，堵终结器时序） | **还是挂** |
| blob 检查 + dispatcher 里加面包屑 fetch（见下） | 绿 2/2 |
| blob 检查 + 其后多插一个任意检查 | 绿 |
| `streaming_upload` 检查（字符串 generator + 150ms promise 挂起，W2 起） | 永远绿 |

面包屑细节（heisenbug 的最强信号）：在 `websocket.ts` 的
`makeDispatchers` 的 onOpen/onMessage 里、`resolveLive` 之前各加一次
RN 全局 `fetch('/report?...')`（fire-and-forget）→ 病消失 2/2。任何
JS 时序/分配扰动都会挪走病灶。

**排除的假设**：服务器侧（30/30 快 + 101 后正常保持）；fetch 泵泄漏/饥饿
（valve 全绿）；纯 GC 压力；RN 原生异步机制单独；Blob 构造单独；Blob
终结器延迟释放（close() 无效）；生成器内 promise 挂起单独（字符串路径
健康）；`fetch_stream_chunk` 零拷 UAF（`chunk.data(rt).to_vec()` 同步拷贝，
lib.rs:605）；id 碰撞（urlsession 注册表 id 是全局原子）；futures-channel
oneshot 的收发顺序（先入队后信号，lib.rs:1528-1533）。

**未单独验证**：FormData blob part（非直接 body）单独是否触发——完整检查
里两者混合，绕过轮只证明了直接 body 路径 sufficient。接手者如需拆，把
blob 检查砍到只剩 form 部分跑一轮。

## 6. 存活假设与下一步（按优先级）

1. **jsi-rs 的 `TurboCallback::call` 跨线程 marshalling**——唯一没拆开的
   黑盒。所有症状（平台无关、只打击 ws 的长寿命回调、fetch 的短命回调
   无恙、任何 JS 扰动即愈）都指向 native→JS 的投递通道在特定时序下丢调用。
   ws 的 dispatcher 在 connect 时创建、数秒后才被调用（跨 GC/分配世代）；
   fetch 回调创建后毫秒级触发。源码在 `/Volumes/YYP_PSSD/Source/jsi-rs`，
   找 `call`/`call_with_no_args` 的实现（从 executor 线程调用时怎么排程
   到 JS 线程）。
2. **`blobBodyStream` 不捕获 Blob 的变体**——fetch.ts 的 isBlob 分支改成
   先启动 `readBlob`（持有 promise），生成器只 yield 那个 promise 的结果。
   如果治好，说明是 Blob 的原生身份被生成器拓活过 fetch 生命周期这件事
   本身有问题（与 close() 无效并不矛盾——close 释放 store 条目，但 JS
   wrapper 对象仍在）。一轮 30 秒实验。
3. urlsession 侧 ws connect 的 `race(open_rx, cancel)`（vendor/
   monet-urlsession/src/ws.rs:73-77）与 W2b 丢失唤醒家族的亲缘性——
   W2b 只修了 body.rs 的有界通道（RoomWaiter），oneshot + cancel race
   的组合没有等价防护。**但注意**：Android 走 monet-ws（tungstenite），
   没有 race 结构，同样犯病——这削弱本假设，除非两平台各有各的唤醒丢失。

一个值得先做的便宜实验：**在 `ws_connect` 的 spawned task 里加 valve_log
打点（Rust 侧改一行 + 重编 3 分钟）**，确认 connect future 到底是没被
唤醒（park 在 race 上）还是醒了但 `on_open.call` 没送达 JS——这直接
把假设 1 和 3 区分开。

## 7. 仪器清单

- **字节级 TCP 代理**（区分 wire 真相与 app 行为的关键仪器）：
  node 脚本，9864→8964、9865→8965 双转发，双向逐字节 latin1 打日志。
  把 App.tsx 的 `DEFAULT_BASE`（iOS 分支）临时改成 `http://127.0.0.1:9864`
  （metro 即时生效），ws 会自动跟到 9865。**跑完记得改回来**。
  脚本本体（`/tmp/ws-proxy.js`，重启会丢，从这里抄）：

  ```js
  const net = require('net');
  const t0 = Date.now();
  const ts = () => ((Date.now() - t0) / 1000).toFixed(3);
  function pipe(port, target, label) {
    net.createServer(client => {
      const server = net.connect(target, '127.0.0.1');
      client.on('data', d => {
        console.log(`[${ts()}] ${label} C->S ${d.length}B: ${JSON.stringify(d.toString('latin1').slice(0, 400))}`);
        server.write(d);
      });
      server.on('data', d => {
        console.log(`[${ts()}] ${label} S->C ${d.length}B: ${JSON.stringify(d.toString('latin1').slice(0, 400))}`);
        client.write(d);
      });
      client.on('close', () => { console.log(`[${ts()}] ${label} client FIN`); server.end(); });
      client.on('error', () => { server.destroy(); });
      server.on('close', () => { console.log(`[${ts()}] ${label} server FIN`); client.end(); });
      server.on('error', () => { client.destroy(); });
    }).listen(port, '127.0.0.1', () => console.log(`[${ts()}] proxy ${label} :${port} -> :${target}`));
  }
  pipe(9864, 8964, 'http');
  pipe(9865, 8965, 'ws');
  ```

  注意：**不要 truncate 正在追加的日志文件**（nohup 的 fd 偏移会产生 NUL
  空洞）——换新文件名。
- **valve log**（泵生命周期，lib.rs `valve_log`）：launch 时带
  `SIMCTL_CHILD_MONET_VALVE_LOG=1`，输出在 app 容器**根目录**的
  `monet-valve.log`（`xcrun simctl get_app_container $SIM com.monet.rndemo
  data` 给出的路径本身就是 HOME，不要再拼 /home）。ws 侧目前无打点。
- **NW 日志**：`xcrun simctl spawn $SIM log stream --level default
  --predicate 'process == "MonetDemo" AND subsystem CONTAINS
  "com.apple.network"'`——看 TCP 连接状态机与 bytes in/out（141/175 这种
  数字能判断握手后有没有发帧）。
- **报告通道纪律**：电池结果从服务器 `/report` println 读
  （`/private/tmp/battery-server.log`）。pass 不上报（只有 fail 和
  `__done__` 总结）；一轮冷启动只有一份主电池总结（TLS 电池手动跑）；
  多设备同时跑会互相污染增量——读数前打 mark，只认增量。

## 8. 当前工作树状态（未提交）

v2 补丁的第二批（content:// + Blob TS 面），git status：

- `packages/monet-rn/src/android_content.rs`（新）：content:// fd 路线，
  **已验证**（Android 干净轮 35·0 含 `formdata_content_uri`）
- `packages/monet-rn/src/bootstrap.rs`：cache dir + content Context 的
  stash 移出 httpengine cfg（fallback 构建也要有）
- `packages/monet-rn/src/lib.rs`：`read_file_chunk` 的 content 分支 +
  `read_chunk` 提取 + `file_path` 报错文案更新
- `packages/monet-rn/ts/blob.ts`（新）：BlobLike/isBlob/readBlob/
  blobBodyStream——**blobBodyStream 是触发路径**
- `packages/monet-rn/ts/fetch.ts`：isBlob body 分支（blobStream）
- `packages/monet-rn/ts/formdata.ts`：blob part（stream/serialize/append）
- `packages/monet-rn/ts/index.ts`：导出 utf8Encode + BlobLike
- `platform/rn-demo/App.tsx`：`formdata_content_uri`（Android 独占）+
  `formdata_blob_part`（双平台）+ utf8Encode 导入
- demo Android 三件套：manifest 的 FileProvider、`res/xml/file_paths.xml`
  （cache-path "fixtures"）、build.gradle 的 androidx.core

**验证状态**：content:// 侧完成（Android 35·0）。Blob 侧：检查自身全过，
但 blob 检查在位时 ws 竞态确定性触发（本文件全部内容）。iOS 当前 34 项
（33 + blob），Android 35 项（+ content）。

**提交建议**：竞态修复前，content:// 半边可单独收（把 blob 相关改动摘出
来）；或者整体挂起等修复。fetch.ts 的直接 Blob body 若要临时摘除，注意
FormData blob part 未单独验证（§5 末尾）。

## 9. 相关历史（别重复踩）

- ws:round_trips 的宿主争抢 flake 有三次历史记录（W3a、W5、W6，见 v2
  plan）——**那是另一个（或叠加的）现象**：构建后忙机超时、空闲复验绿。
  本次不同：空闲机器上确定性失败、且 iOS 也失败。别用老结论解释新病。
- 上午（补丁前）同电池 iOS 33·0 ×2、Android 33·0 ×2——那时 blob 检查
  不存在，潜伏竞态没被踩中。
- W2b 的丢失唤醒（RoomWaiter，crates/monet-core/src/body.rs）是同族病：
  futures-channel 的唤醒槽被同步探针清空。修过的地方只有 body.rs 的
  有界通道。
