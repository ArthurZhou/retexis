# 设计笔记

这份文档记录**为什么这么做**，以及改动时必须守住的约束。功能说明看 [README](../README.md)。

---

## 一、八条不可破坏的约束

任何改动如果违反了下面任何一条，这个项目就不再是它该有的样子了。

### 1. 路由层不解析 payload

`retexis-routing` 只读 `Envelope` 的头部和 `Body::Data` 的 `channel` / `content_type`，
**永远不读 payload 的字节**。一旦路由层开始理解业务内容（比如"这是聊天消息，需要重传"），
它就无法再承载二进制、流式或未来任何一种传输。

### 2. 数据层不碰路由头，应用层不碰协议

`retexis-data` 只看到 `Delivery`（谁发的、哪个 channel、什么内容、走了几跳）。
它不知道 `LinkId`、不知道下一跳是谁、不知道 TTL。它只会说"把这个发给某人或所有人"。

`retexis-app` 只看到 `Frame` 和 `StreamEvent`。它不构造信封、不选 content_type、不知道
channel 号从哪来。**加一个应用（pty、文件、指标）就是写一个 `DataSink`**，不碰协议。

### 3. 介质拥有分帧权

`Link::send(&self, envelope: &[u8])` 收到的是**一整段已编码的信封字节**，没有长度前缀、
没有分隔符。加长度前缀是 TCP 的事，切成 200 字节小块是 radio 的事，加 WebSocket 帧头是
ws 的事。**如果哪天介质需要路由层帮它分帧，说明抽象漏了。**

推论：`Link::max_frame()` 是介质声明的硬上限，路由层据此拒绝装不下的信封并上报
`DropReason::TooLarge`。不会静默截断。

### 4. 介质能力必须有默认实现

`LinkFactory::beacon()` 默认返回 `NoBroadcast`。任何新增的能力都要遵循同样的形状：
**默认值是"我做不到"，而不是"必须实现"**。否则每加一种介质就要改一遍所有已有介质，
那说明这个能力放错了层。

### 5. 只有 router 线程写路由状态

`neighbors` / `table` / `names` / `dedup` / `pending` / `stats` 只有 router 线程能改。
其它线程通过 `mpsc<Event>` 发消息、通过 `RwLock<MeshStatus>` 读快照。这条保证了下一条。

### 6. 任何转发都必须先过去重

`on_routable` 的第一件事是 `dedup.mark(env.id)`。去掉它，环网立刻变成广播风暴；
路由表暂时不一致时单播会死循环。TTL 是最后一道防线，不是第一道。

### 7. 认证必须在解码之前

`on_inbound` 里验签发生在 `Envelope::decode` 之前，而且策略是全象限显式的：没有密钥就
不接受带签名的包，有密钥就不接受明文包。**任何"验不过就降级为明文"的写法都是漏洞**，
因为攻击者只需要发一个不带 tag 的包。

### 8. 去重不能吃掉重传

去重缓存记录的是"这个 message id 我处理过"。但一个**中继**再次看到同一个单播，
含义完全不同：发送方没收到 ACK，而我上次的转发可能根本没送达（下一跳当时是断的）。
如果中继像丢重复包一样丢掉它，重传机制在最需要它的场景下完全失效。

所以：重复包在**目的节点**不重复投递（但重发 ACK），在**中继**继续转发；广播的重复包
一律丢弃（广播从不重传，所以重复只可能来自环路）。这条区分是第 5 条的必要补充，
不是例外——它保证的是"去重抑制的是环路，不是重传"。

---

## 二、一个包的一生

以 carol 单播给 alice（两跳，中间是 bob）为例：

```
carol REPL
  /msg alice "..."
        │
        ▼
  handle.resolve("alice")            ← 查 MeshStatus.names，名字是路由通告带回来的
        │ NodeId
        ▼
  handle.send_reliable(...)          ← 打上 FLAG_ACK_REQ，登记到 pending
        │ 构造 Envelope{ src=carol, dst=Node(id), ttl=8, hops=0, flags=ACK_REQ }
        ▼
  Event::SendReliable(env) ──mpsc──> carol 的 router 线程
        │
        ├─ dedup 记一笔（防止自己的包绕回来再处理）
        ├─ pending[msg_id] = { attempts: 1, next_at: now + base }
        ├─ dst != 自己 → 不本地投递
        ├─ ttl 8 → 7，hops 0 → 1        ← 第一跳也算一跳
        └─ 查表：alice 的 next_hop 是 bob 那条链路
              │
              ▼  send_via(): 打 FLAG_AUTH → encode → seal(追加 16B tag)
        ┌─────────────────────────────────────┐
        │ TCP 帧: u32 长度 + 信封字节 + tag    │   ← 分帧发生在介质层
        └─────────────────────────────────────┘
              │
              ▼
bob 的链路读线程  link.recv() → Event::Inbound{link_id, bytes}
              │
              ▼
bob 的 router 线程
        ├─ 验签（有密钥就只收带 tag 的）
        ├─ decode → Envelope
        ├─ dedup 首次 → 继续
        ├─ dst=alice != bob → 不本地投递
        ├─ ttl 7 → 6，hops 1 → 2
        └─ 查表：alice 的 next_hop 是 alice 那条链路，且不是来路 → 转发
              │
              ▼
alice 的 router 线程
        ├─ 验签、decode、dedup 首次
        ├─ dst == 自己 → deliver()
        │     Delivery{ from=carol, from_name=Some("carol"), hops=2, .. }
        ├─ 包要 ACK → send_ack()：构造 ack(src=alice, dst=carol, acked=msg_id)
        │     走同样的路由逻辑，反过来两跳回去
        └─ 单播且是给自己的 → 不再转发
              │
              ▼
carol 收到 ack → pending.remove(msg_id) → notice "v delivered ... (1 attempt)"
```

如果 ack 丢了，carol 的重传定时器会到期，用**同一个 message id** 再发一次：
alice 的 dedup 认出它 → 不重复投递，只再回一个 ack。幂等，不需要额外机制。

广播走同一套路径，区别只在 `flood()` 会发给**除来路以外的所有链路**，
并且每个节点在投递本地之后继续转发。

---

## 三、几个刻意的选择

### 不用 async

传输核心要的是可预测：每个包做了什么、什么时候阻塞、内存峰值多少。
tokio 会让"这个 `await` 点在哪"变成阅读理解题，而且在这个规模下（每节点几十条链路）
线程模型的开销可以忽略：**一条链路 = 一个读线程 + 一把写锁**。

代价：万级连接需要换 async。那时要改的是 `Router::spawn_reader` 和 `Link` 的实现，
上三层完全不动 —— 这是分层带来的好处。

### 手工编码，不用 serde

- 线格式是协议的一部分，应该有一份人能读的规范（README 里那张表），而不是 `bincode`
  的隐式布局。换个语言实现时不用去读 serde 的 derive 输出。
- 固定 64 字节头让窄带介质能只看头判断装不装得下，不用先反序列化。
- 零依赖：`cargo build` 不联网，编译 20 秒，没有供应链风险。

代价：加字段要同时改 encode/decode 和文档。所以线格式里有 `version` 和 `flags`，
破坏性变更靠 `version` 区分。

### 密码学用 RustCrypto，不用 ring

一开始 SHA-256 和 HMAC 是手写的（217 行），理由是"零依赖"。后来换掉了——手写密码学是
公认的反模式，而向量测试只能证明**实现正确**，证明不了**没有侧信道**。

选 `sha2` + `hmac` 而不是 `ring` / `aws-lc-rs`，理由是具体的：

| | RustCrypto (`sha2`+`hmac`) | `ring` / `aws-lc-rs` |
|---|---|---|
| 语言 | 纯 Rust | C + 汇编 |
| 构建需要 | 只要 rustc | C 工具链、`cc`、有时还要 cmake |
| `no_std` | 可以 | 不行 |
| 审计 | 广泛使用、有专门审计 | 更成熟 |

对一个可能跑在 LoRa 节点上的传输核心，"不需要 C 工具链"比"更成熟的汇编实现"更重要。
代价是传递依赖从 0 变成 10 个纯 Rust crate（`digest`、`block-buffer`、`generic-array`、
`typenum`、`subtle`、`cfg-if`、`cpufeatures` 等），全部无 C。

三条约束保证这次替换是安全的：

1. **`Sealer` 接口不变**。换库只动了 `retexis-crypto` 一个文件，`retexis-routing` 一行没改。
2. **RFC 向量测试全部保留**。它们现在反过来验证"换库没有改变行为"——同样的输入必须出同样的字节。
3. **冻结的线格式快照还在跑**。`wire_vectors.txt` 里那 3 个 HMAC 用例必须逐字节匹配，
   所以线格式的 tag 一个 bit 都没变。

`verify` 用的是 `verify_truncated_left` 而不是 `verify_slice`：后者要求 tag 长度等于完整输出
（32 字节），我们用的是 16 字节截断 tag，用错了会永远返回 false——这个坑我踩过一次，
测试立刻抓住了。

### 单线程 router

路由状态是典型的"多处读、单处写、需要原子地一起变"。用锁的话，`table` 和 `neighbors`
要么用一把大锁（等于串行），要么用多把锁（就要处理锁顺序和中间状态不一致）。
单线程事件循环直接消灭这个问题：**没有并发，就没有竞态**。

读路径用 `RwLock<MeshStatus>` 快照，每处理完一个事件刷新一次。写者只有一个，读者不会饿死。

### 名字跟着路由走

路由通告的条目是 `(节点 id, 距离, 名字)`，不是 `(节点 id, 距离)`。
看起来是冗余，但否则两跳之外的节点只有一串十六进制，`/msg alice` 就没法用。

代价：一个节点可以谎报别人的名字。名字是**便利标签，不是身份**，身份是 `NodeId`。

### 重复链路用 (min, max) 排序，而不是方向

两个节点同时拨对方，或者重连撞上一条正在死的 socket，就会出现多条链路。
必须收敛到一条，而且**两端的选择必须一致**，否则双方互相关掉对方的幸存者，然后无限重拨。

一开始我用"保留节点 id 较小一方发起的那条"——只按方向区分。这在恰好两条链路时是对的，
四条链路时就崩了：所有同向链路都满足规则，一条都关不掉。测试抓到了这个。

正确做法是给每条物理链路一个两端都能算出的全序键。`hello` 里带上发送方这条链路的本地 id，
于是两端都知道同一条链路的两个 id，`(min, max)` 就是同一个键，保留最小的那条。
这个键是完整的（任意两条不同链路键必不同）且对称的，所以一定收敛到唯一一条。

### 泛洪抑制只做本地决策

协作式方案（MPR）能省更多：每个节点选一组邻居覆盖自己的两跳邻域，只有被选中的转发，
覆盖不损失。但 MPR 需要邻居之间交换二跳拓扑并保持一致，复杂度和故障模式都高一个量级。

令牌桶是纯本地的：不协商、不依赖别人的状态、不会被别人的错误决策拖下水。代价是过载时
必然牺牲覆盖——任何抑制都会丢掉某个子树。这是个诚实的天平：**默认预算宽松到正常负载
完全不触发，只有真的过载时才牺牲广播覆盖率换取网络存活**。模拟器把这个天平整量化了
（480 条广播的过载下，-58% 转发量换 -37% 送达率）。

---

## 四、解耦：审计结果与可执行的约束

"三层解耦"这种话必须有证据。审计的办法是看依赖方向，而不是看注释里怎么写的。

### 结论

| 检查 | 结果 |
|---|---|
| `retexis-core` 的 `[dependencies]` | 空。它只是词汇表（信封、ID、trait） |
| `retexis-routing` 依赖 | 只有 core + crypto，**没有任何介质** |
| `retexis-app` 依赖 | core + routing，不知道字节怎么走 |
| 介质 crate 依赖 | 只有 core（+ discovery），不反向依赖路由 |
| 线格式里的介质假设 | 无。固定 64B 头 + `max_frame` 由 trait 协商 |
| 路由层里的介质分支 | 无 `if tcp` / `if udp`，只有 `Link` 的方法调用 |

### 审计抓到的两处真实泄漏（已修）

1. **拨号路径绕过了注册表**。`main.rs` 里写着 `Dialer::new(Box::new(TcpFactory::new()), url)`
   —— 也就是说 `--connect udp://...` 会拿 TCP 去连。注册表是存在的，但只用在 listen 一侧。
   修法：注册表持有 `Arc<dyn LinkFactory>`，新增 `factory_for(url)`，拨号和监听走同一条
   解析路径。这个 bug 在加 UDP 之前一直躺着，因为只有一种介质时它不可见。
2. **介质名称进了通知枚举**。`RouterNotice::LinkUp { transport: &'static str }` 让路由层
   知道"介质有名字"。这一条**保留**：它只是日志里的一个标签，去掉它就得让 UI 去猜。
   写在这里是为了说明"完全解耦"是有代价的，我们选择留一个字符串。

还有一处**刻意不修**：`Link::initiator()` 是拨号器的概念（谁主动发起连接），放在介质
trait 上并不纯粹。但重复链路仲裁要用它，而且所有介质都能如实回答，所以留着。

### 两个应用，一层管道

后来加了 `retexis-gui`（桌面聊天）。它没有变成第二份协议，因为应用层本来就是"写一个
`DataSink`"的地方——`ChatSink` 和终端里的 `ChatApp` 是同类东西，只是把消息写进时间线而不是
打到 stdout。

有意思的是它暴露了两处**重复**，都在应用层之上：

| 重复 | 处理 |
|---|---|
| 拨号逻辑（按 scheme 找介质、去重、退避） | 下沉到 `retexis-routing::Connector`，两个应用共用 |
| "问所有 beacon 谁在"（并行查询、合并、去掉自己） | 下沉到 `retexis-discovery::scan`，两个应用共用 |
| HTTP + JSON（请求解析、百分号解码、响应写出） | 抽成 `retexis-http`（现在只有模拟器用） |

前两条都不是"提前设计"的：是写第二个应用时，发现自己在抄第一份代码，才把它搬下去的。
搬的方向永远是向下——应用之间不互相依赖，`decoupling.rs` 里有一条断言盯着这一点。

`retexis-http` 因此是个零依赖的叶子 crate，和 `retexis-core` 一样：它不知道什么是 mesh，
只知道怎么把一段字节读成请求、把一个响应写回去。

### 窗口为什么改变了依赖策略

`retexis-gui` 最初把界面放在浏览器里，靠的就是上面那个 `retexis-http`。改成原生窗口之后，
它换成了 `Slint`，并且只用**软件渲染器**：界面画进一块普通内存缓冲，再 `BitBlt` 到窗口，
没有 GPU、没有 OpenGL，仍然不需要 C 工具链，但代价是**依赖数量**：

| | 协议核心（11 个 crate） | `retexis-gui` |
|---|---|---|
| 第三方 crate | 10 个（RustCrypto 那一套） | 242 个（Slint + winit + 字体栈） |
| `rust-version` | 1.75 | 1.95 |
| 冷编译 | 约 20 秒 | 约 2 分钟 |
| C 工具链 | 不需要 | 不需要（`windows-sys` 只是 API 声明） |

所以 `retexis-gui` 单独把 `rust-version` 提到 1.95，其余 crate 不动：协议核心还能往小目标上
编，窗口只在你桌面上跑。这条边界是靠 `Cargo.toml` 的 per-package `rust-version` 表达的，
不是靠约定——`cargo` 自己会拒绝在旧编译器上构建它。

`retexis-http` 没有因此删掉：模拟器的控制 API 还在用它，而那是个浏览器页面，正好合适。

### 界面层分三块，各自只认一件事

| 文件 | 知道什么 | 不知道什么 |
|---|---|---|
| `model.rs` | 日志条目 → 一行文字（谁、多久以前、几个字节、送没送到） | 节点、线程、窗口 |
| `chat.slint` | 布局、命中测试、动画、焦点 | 什么是 ACK、什么是一条流 |
| `bridge.rs` | 把回调翻译成一次 `App` 调用，把节点状态翻译成行模型 | 任何几何尺寸 |

`model.rs` 是纯函数，所以"3 秒前"和"DELIVERED · 580 B"是**测出来**的，不是截图看出来的。
`bridge.rs` 用一个 100ms 的定时器读节点，而不是让协议往 UI 推事件：日志本来就是加锁的环形
缓冲，轮询比给每个事件开一条通道便宜，而且因为每次重读一小段尾巴，晚到的 ACK 仍然能改到
它自己那一行。

界面状态里唯一刻意收窄的是对话框：`dialog` 是一个 `int`（0/1/2），不是两个布尔。两个布尔
可以同时为真，于是窗口会在 DISCOVER 的正文上画一个 CONNECT 的标题——这个状态现在写不出来。
（反过来，"隐藏的正文块仍然占布局高度"这个坑是 `visible:` 的语义，不是状态的问题，
记在 README 的坑里。）

### 把结论变成测试

架构会漂移，除非有人拦着。`crates/retexis-routing/tests/decoupling.rs` 直接读各个
`Cargo.toml`：

```rust
assert!(!production_dependencies("retexis-routing").contains("retexis-link"));
assert!(production_dependencies("retexis-core").trim().is_empty());
```

依赖图就是架构。有人往路由层里加一句 `use retexis_link_udp::...`，这个测试立刻红，
而编译器不会报错。

### 加一种介质要改什么（加 UDP 时实测）

| 位置 | 改动 |
|---|---|
| `retexis-link-udp` | 新 crate：`Link` + `LinkAcceptor` + `LinkFactory` |
| `retexis-node/main.rs` | 一行 `registry.register(Arc::new(UdpFactory::new()))` |
| `retexis-routing` | **0 行** |
| `retexis-core` | **0 行**（`beacon()` 有默认实现） |
| `retexis-app` | **0 行** |

UDP 还顺手证明了另一件事：**分帧权在介质手里是对的**。数据报本身有边界，所以 UDP 介质
不需要长度前缀、不需要重组、没有半包状态——而路由层完全没察觉区别。

### 发现为什么是介质的能力

"附近有谁"是介质属性：以太网能广播，点对点不能，模拟交换机根本不存在"附近"。所以
`Beacon` 挂在 `LinkFactory` 上，而不是塞进路由层。

关键是**默认实现**：

```rust
fn beacon(&self) -> Box<dyn Beacon> {
    Box::new(NoBroadcast::new(self.scheme()))
}
```

`NoBroadcast` 的 `broadcast` 返回 `Ok(0)`、`discover` 返回空表。这不是错误路径，是正常
答案——"这个介质问不到人"。于是 `--discover` 可以对所有注册介质各问一次，不需要任何
`if scheme == "..."` 的特判，新加的介质也不会因为"忘了实现广播"而编译不过。

TCP 的 beacon 值得单说：TCP 不能广播，但**宣告一个 TCP 端点**可以用 UDP 广播。所以
发现通道永远是 UDP，载荷里写 `tcp://` 还是 `udp://`。这也是为什么 `retexis-discovery`
是个独立 crate 而不是塞进某个介质里——两个介质指向同一份实现。

---

## 五、线格式快照：留字节，不留实现

项目里曾经有一份 Python 的第二实现（`sim/`），带 9 个场景和一套离散事件网络模型。它已经删除。
`crates/retexis-core/tests/wire_vectors.txt` 留下来了，因为它留下的东西本来就不是"实现"。

### 留下的是什么

10 个信封 + 3 个 HMAC 用例，每个是 `FIELD`（字段）+ `HEX`（字节）。测试从字段重建信封、
编码、逐字节比对，再解码回读。**两个方向都被钉住。**

它现在的角色是**格式漂移告警**，不是互操作证明：字段挪一个字节、`VERSION` 翻一位、
tag 长度改一下，`cargo test` 立刻红。没有它，这种改动要到和旧节点联调时才发现。

代价是它**没有生成器**了。改格式就得手改 `HEX` 行，并且认真看 diff——这不是缺陷，
是把"兼容性成本"显式化：那个 diff 就是所有已部署节点要付的代价。

### 为什么删掉那份实现

它确实有产出。两个真 bug 是它先发现的，都不是读代码想出来的：

1. **单次 `Hello` 丢失 → 链路永久半识别**。`a` 不知道 `b` 是谁，于是既不发路由也不泛洪，
   链路静默死掉。TCP 上内核会重传，所以这个问题在 TCP 上永远看不见；换成 radio 是致命的。
   修复仍在：`Hello` 变请求-应答，未识别的链路每个通告周期重发，200ms 防放大窗口。
2. **中继把重传当重复包吃掉**。carol 经 bob 发给 alice 的 DM，恰逢 bob 到 alice 的链路抖动，
   bob 缓存了 msg id 然后丢弃所有重传，消息永远到不了。
   修复仍在：去重只抑制广播，单播重复包在中继处继续转发。

但它的定位一直是"另一份实现"，这决定了它**不能**回答"生产代码跑起来对不对"。而这个问题
现在由 `retexis-sim` 回答——每个节点就是一个真 `Router` 线程，走的是生产路径。于是成本
开始大于收益：

| 成本 | 具体是什么 |
|---|---|
| 同步 | 每次改协议要手工改两份，漏了就漂移 |
| 断言错层 | 9 个场景断言的是**模型**的行为，模型对了不代表实现对了 |
| 假阳性 | 模型里通过的场景，可能是模型自己写错了 |
| 维护面 | 一份 2000 行的协议实现 + 一套 HTTP 服务 + 一套 UI 后端 |

第三行是关键：第二份实现最大的风险不是它出错，而是**它和你一起错**——两边都实现成一样的
误解，测试全绿，而规范其实是另一回事。它抓到的两个 bug 都属于"一边对一边错"，
这类问题它擅长；"两边都错"它一点忙都帮不上。

### 第二份实现留下的三个 UI bug

那套 Python 版也带过一个浏览器 UI，逼着去点测试没覆盖的路径，于是：

1. **死节点还留着路由表**。`go_down()` 清了链路和 pending，忘了清 `table`，界面上一个
   "已停止"的节点还显示着 5 条路由——它已经不可能用上了。
2. **改名不传播**。名字在握手时学到，之后的路由通告里虽然带着名字，但接收方跳过了
   "发送者自己那条"，于是改名永远传不出去。修法（Rust 侧同样生效）：通告是权威来源，
   发送者的自身条目也要更新名字。
3. **节点自己的广播显示成十六进制 id**。名字表里没登记自己。

第 1、2 条的修复在 Rust 里仍然有效；第 3 条是那个 UI 的局部问题，随实现一起消失。

---

## 六、Rust 模拟器：一台虚拟交换机

`retexis-sim` 里没有第二份协议。每个节点就是一个 `retexis_routing::Router` 线程：

```
浏览器  ──HTTP/JSON──▶  http.rs  ──▶  sim.rs  ──▶  hub.rs
  画布/检查器/日志        路由分发      展示状态      虚拟交换机
                                                      │
                                        ┌─────────────┴─────────────┐
                                        ▼                           ▼
                                  Router 线程 (alice)          Router 线程 (bob)
                                  握手/路由/去重/重传            同上
```

### 为什么这件事几乎没花力气

因为硬件解耦层的边界当初就是按"介质可替换"画的。模拟器要做的全部工作，是实现 `Link`：

```rust
fn send(&self, envelope: &[u8]) -> Result<(), LinkError>;  // 交给交换机
fn recv(&self) -> Result<Vec<u8>, LinkError>;              // 阻塞等队列
fn shutdown(&self);                                        // 拔线
```

Router 分不出它和 TCP socket 的区别。`retexis-routing` 为模拟器改的代码量是 **0 行**。
这也是"加一种介质不用动路由层"这个说法的第一次真实检验——之前只有 TCP 一个实现，
那种说法是没有证据的。

### 交换机怎么让"断链"是真的

这是整个设计里最值得说的一点。切断一条链路时，交换机**不是**设个标志让 UI 画条断线，
而是把两个端口的 `Sender` 丢掉。两端阻塞在 `recv` 上的线程立刻拿到 `Err`，Router 于是走它
本来就该走的路径：`LinkDown` → 删掉经由该链路的路由 → 待定单播继续重传直到 `DeliveryFailed`。

所以界面上看到的删路由、重传失败、邻居遗忘，全是协议在处理一个真实的链路错误，不是模拟器
演出来的。同理，"杀死节点"是真的 `handle.shutdown()`——Router 会给邻居发 `Bye` 然后关闭所有
链路；"复活"是同一个 `NodeId` 起一个新 Router，因此计数器归零、路由表从空开始。

### 延迟为什么不用轮询

每帧进一个按到期时间排序的 `BinaryHeap`，由一条专门的线程按"下一个到期时间"精确等待。
轮询会让 5ms 的链路变成 25ms，而延迟是本项目要观察的量。有测试量它：150ms 的链路，
发送到进收件箱 ≥120ms。

暂停时堆里的帧不会被丢掉，恢复时把每个到期时间整体平移暂停时长——否则一恢复就会有
一整批帧同时炸出来，看起来像一次风暴，而那不是协议的行为。

### 意图和现实要分开

每条链路有两个布尔：`admin_up`（操作员想让它通）和 `up`（现在真的通）。分开之后：

- 切断 = `admin_up=false`，监督线程不再重插，分区就一直存在
- 节点重启 = `admin_up` 保持 true、`up=false`，受监督的链路会自动回来

这个区分抓到了一个 bug：早期版本的"愈合"是先比较 `up` 字段和当前意图，相同就跳过——
而重启后的链路正好是"意图通、实际不通"，于是按愈合什么都不会发生。现在 `up` 字段永远执行，
靠 `set_up` 自身幂等。

### 诚实地说，模拟器没有覆盖什么

- **`dial::Dialer` 没被跑到**。网线是交换机插的，没有拨号过程，所以指数退避重拨这条路径
  只有 `retexis-routing` 的集成测试覆盖。模拟器里的"断线自动重连"是交换机的退避重插，
  语义相同但代码不同。
- **TCP 分帧没被跑到**。`HubLink` 是消息边界，不存在粘包。
- **`rate` 只缩放介质延迟**。路由通告的定时器在 Router 里，模拟器没有权力改协议的节奏。

这三条都写在 README 的已知限制里，不藏着。

---

## 七、故障模式

| 现象 | 实际发生什么 |
|---|---|
| 对端进程被杀 | TCP 读线程拿到 EOF → `Event::LinkLost` → 删除该链路 → 删除所有经由它的路由 → notice 上报；拨号器开始退避重拨 |
| 对端主动 `/quit` | 先发 `Bye`（对端立即清理，不用等 TCP），再关闭 socket |
| `/kick` | 和链路死亡走同一条路径（`Event::DropLink`），所以可以用来演练重连 |
| 网络闪断（未 FIN） | 读线程每 250ms 醒一次检查 closed 标志；真正断链靠 TCP 超时或路由过期 |
| **单次 Hello 丢失** | 未识别的链路每个通告周期重发 Hello；收到 Hello 一定回一个（200ms 防放大窗口）。没有这个，链路会静默半死 |
| 路由过期 | `route_timeout` 内没收到刷新 → 删除 → 单播退化为泛洪 |
| 路由表暂时不一致 | 单播查不到路由 → 泛洪（等于路由请求）；目的节点的通告沿原路返回补齐 |
| 环网广播 | 每个节点按 `message id` 去重，只处理一次；`Flooded` 计数可见 |
| **ACK 丢失** | 发送方重传同一个 id，接收方认出重复 → 重发 ACK，不重复投递（`duplicate_acks` 计数） |
| **中继的下一条跳断了** | 中继缓存了 msg id 但转发失败；发送方重传时中继把它当重传继续转发（不是当重复包丢弃） |
| 重传用尽 | `DeliveryFailed` notice + `pending` 清理，REPL 打印 `x failed ...` |
| **密钥不对** | 握手就被挡在验签那一步，拿不到任何路由，收不到任何流量 |
| **明文节点撞已认证 mesh** | 双向都拒绝：它验不了别人的签名，别人也不收它的明文 |
| 两个节点同时拨对方 | `(min, max)` 链路键收敛到唯一一条，两端选择一致，不振荡 |
| 信封大于介质上限 | 该链路拒发 + `Dropped{TooLarge}`，其它链路不受影响 |
| 自己连自己 | `hello` 里发现 `src == 自己` → 关掉这条链路并提示 |
| 半开连接 / 端口扫描 | `accept()` 出错不杀监听线程，睡 50ms 继续 |
| 泛洪过载 | 令牌桶丢掉多余的中继，`flood_suppressed` 计数；本地投递不受影响 |

---

## 八、加东西的检查清单

### 加一个介质（http / ws / radio）

1. 新建 crate `retexis-link-xxx`，只依赖 `retexis-core`。
2. 实现 `Link`：`send` 里自己做分帧，`recv` 里自己解帧。
   - **`recv` 必须是阻塞语义**，返回一个完整信封；内部可以用短超时轮询来响应 `shutdown`
     （Windows 上 `shutdown` 唤不醒阻塞的 `recv`，这是实测过的）。
   - **`send` 必须可多线程调用**（router 线程可能并发写同一链路）。
   - `initiator()` 要如实回答（`/peers` 会显示，重复链路诊断也靠它）。
   - 窄带介质：`max_frame` 设成真实 MTU，`send` 里切片，`recv` 里重组。
3. 实现 `LinkAcceptor`（`accept()` 返回 `Box<dyn Link>`）和 `LinkFactory`（`scheme()` 返回 URL 前缀）。
4. **决定能不能发现**。能广播就覆盖 `beacon()` 返回 `BroadcastBeacon::new(scheme, port)`；
   不能就什么都不写，默认的 `NoBroadcast` 会如实回答"问不到人"。
5. 在 `retexis-node/src/main.rs` 里 `registry.register(Arc::new(XxxFactory::new()))`，一行。
6. 加测试：握手、粘包/半包、超大帧、对端关闭；如果实现了 beacon，再加一个"能应答查询"的测试。

**不需要改**：`retexis-core`、`retexis-routing`、`retexis-app`、`retexis-crypto`。

### 加一个应用（pty / 文件 / 指标）

1. 在 `retexis-app` 里实现 `DataSink`：`name()`、`channels()`、`on_frame()`；
   需要流就再实现 `on_stream()`。
2. 发送侧用 `retexis_data::frame::send` / `send_reliable`，流用 `StreamSender`。
3. 在 `cli::run` 里 `host.register(Box::new(YourSink::new(...)))`。
4. 如果它有自己的 channel 名，加一个常量；不需要动线格式。

**不需要改**：`retexis-data`（除非要加新的流语义）、线格式、路由层、介质层。

### 改流的线格式

流的三个 header（open/chunk/close）定义在 `retexis-data/src/stream.rs`，**不在信封里**。
所以改它不用动线格式快照，也不会让旧节点解出垃圾——但会让新旧节点互相收不全，
所以要么加版本字节，要么接受不兼容。

### 改发现协议（`Announcement` 的字段）

1. 改 `retexis-core/src/discovery.rs` 的 encode/decode，**同时改 `PROBE_MAGIC` 的版本位**
   （`RXD1` → `RXD2`），否则新旧节点会互相解出垃圾。
2. 加一个"旧版本被拒绝"的测试。
3. 发现协议不在 `wire_vectors.txt` 里，所以不需要动快照。

### 改协议（任何字段）

1. 改 `retexis-core/src/envelope.rs`（encode + decode + 文件顶部那张字节表 + 单元测试）。
2. **同步 `tests/wire_vectors.txt`**。没有生成器：手改 `HEX` 行，或者临时写个脚本从新编码器
   导出再删掉脚本。改完跑 `cargo test -p retexis-core`。
3. 如果 `VERSION` 变了，在 README 的已知限制里写清"旧节点会 `BadMagic`/拒绝"。
4. 如果改了状态机，补一个 `retexis-routing` 集成测试 + 一个 `retexis-sim` 端到端测试。

---

## 九、性能备注

当前路径上的分配（每个转发包）：

- `recv` 里一次 `Vec` 分配（帧长度）+ `rbuf` 的 `drain` 移位
- `encode` 一次 `Vec` 分配（发送）
- 认证开启时 `seal` 再复制一次
- 广播时每条出链路一次 `clone + encode + seal`

要上高吞吐时按这个顺序做：

1. **链路写侧加有界队列 + 独立写线程**。现在一个卡住的 TCP 写会拖住整个 router 线程，
   进而拖住所有转发。这是最重要的一条，也是架构上唯一必须改的地方。
2. 广播时只 encode 一次（`send_via` 里现在对每条链路重复做），改成 `flood` 外层做一次。
3. `rbuf` 换成 ring buffer，避免 `drain` 的 memmove。
4. 认证改成流式 HMAC（增量喂 header 和 body），少一次整包复制。

`pending` 表在正常使用下只有个位数条目；重传定时器按最近到期时间计算唤醒，
空闲时不会轮询。
