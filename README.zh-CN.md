# rtmp-forwarder

一个使用 Rust 编写的轻量 RTMP 接入与转发服务。

`rtmp-forwarder` 接收一路 RTMP 推流，可选中继到原始目标地址，并将流转发到多个 RTMP 上游目标。同时提供内置 Web 控制台和播放能力。

## 功能

- RTMP 接入服务（`listen_addr`，默认 `127.0.0.1:1935`）
- 可选的推流鉴权（`server.app` 和 `server.stream_key`）
- 多目标 RTMP 转发（支持按目标启用/禁用）
- 中继模式基于独立转发连接实现（独立 RTMP 握手/发布）
- 支持 SOCKS5 代理模式接收推流（客户端将代理地址指向本服务监听地址，可用于截流）
- 支持从 SOCKS5 请求中提取原始推流目标地址
- Linux 下支持通过 REDIRECT/DNAT 提取原始目标地址（`SO_ORIGINAL_DST` / `IP6T_SO_ORIGINAL_DST`）
- 内置 Web 控制台管理运行配置
- 转发目标可直接在 Web 界面管理
- Web 界面可查看原始推流地址并直接添加为转发配置
- 中继目标可复用捕获到的原始推流地址
- 支持 Web/API 基础认证（Basic Auth，可选）
- HTTP-FLV 播放（`/live/stream.flv`）
- 可选 HLS 播放列表输出（`/live/stream.m3u8`）
- 单路活动推流设计（同一时间仅支持一路推流）

## 依赖要求

- 支持 Rust 2024 Edition 的工具链
- Cargo

## 启动行为

- 首次启动时，如果配置文件不存在，会自动生成。
- 默认配置路径为 `config.json`。
- 可通过 `-c` / `--config` 指定自定义配置路径。

## 命令行参数

```text
-l, --listen <ADDR>   RTMP 监听地址（默认：127.0.0.1:1935）
-d, --dest <URL>      RTMP 转发目标（可重复传入）
-r, --relay <ADDR>    中继目标地址覆盖
-w, --web <ADDR>      Web 控制台地址（默认：127.0.0.1:8080）
-c, --config <FILE>   配置文件路径（默认：config.json）
    --log <LEVEL>     日志级别（info、debug 等）
    --hls             启用 HLS 相关输出
```

* 命令行参数优先级高于配置文件，启动时会将最终生效配置回写到配置文件。
* 使用 `--dest` 会在启动时覆盖配置文件中的 `forwarders` 列表。
* HLS 播放列表接口：`GET /live/stream.m3u8`（仅在启用 `--hls` 时可用）。

## 配置说明

默认配置文件：`config.json`（或通过 `-c/--config` 指定）。

### 不能通过 Web 界面配置

- `server.listen_addr`：RTMP 接入监听地址
- `server.app`：可选，设置后仅接受匹配 app 的推流
- `server.stream_key`：可选，设置后仅接受匹配 stream key 的推流
- `web.addr`：Web 控制台监听地址
- `web.username` / `web.password`：可选，任一设置即启用 Basic Auth
- `log_level`：默认日志过滤级别

### 可以通过 Web 界面配置

- `forwarders`
- `relay_addr`
- `relay_enabled`

中继行为与普通转发器一致（独立外连 RTMP 会话）。默认从当前推流会话派生目标/app/stream 上下文。

## 原始目标地址与中继

- 在 SOCKS5 代理模式下，服务可从 CONNECT 请求中提取原始 RTMP 目标地址。
- 在 Linux 上，服务也可从 REDIRECT/DNAT 流量中读取原始目标地址（`SO_ORIGINAL_DST` / `IP6T_SO_ORIGINAL_DST`）。
- 当前原始目标地址会在 Web 界面展示，并可直接添加为转发目标。
- 中继模式不是透明透传，而是独立建立 RTMP 外连会话。
- 中继模式下，目标地址可来自捕获到的原始地址（或 `relay_addr` 覆盖），推流上下文来自当前推流元数据。

## 日志

- 配置文件：`log_level`
- 命令行覆盖：`--log`
- 环境变量覆盖：`RUST_LOG`

`RUST_LOG` 优先级最高。

## 安全说明

- 不要提交生产环境的推流密钥、中继地址或认证信息。
- Web/API 支持 Basic Auth，但明文传输下安全性有限。
- 若需公网访问，建议置于 TLS（如反向代理）与网络访问控制之后。
- 本项目代码未经过安全审计，强烈建议仅在可信环境（内网或防火墙后）部署使用。

## 许可证

本项目采用 Apache License 2.0 许可证，详见 `LICENSE`。

## AI 免责声明

本项目部分代码由 AI 生成或辅助生成，可能存在缺陷、安全问题或不完整逻辑。使用者需自行完成审查、测试与适配验证。作者与贡献者按 "as is" 提供本项目，不对任何直接或间接损失承担责任。
