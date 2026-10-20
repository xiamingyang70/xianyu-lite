# xianyu-lite

**闲鱼多账号自动化助手 · 精简单机版** — 开箱即用，无需安装 MySQL / Redis，一条命令跑起来。

基于开源项目 [zhinianboke/xianyu-auto-reply](https://github.com/zhinianboke/xianyu-auto-reply)（AGPL-3.0）裁剪而来，遵循同一协议开源。

## 这是什么

原版 xianyu-auto-reply 功能全面但部署较重（MySQL 8 + Redis + 多进程）。很多个人卖家 / 小团队只想：

- 挂着几个闲鱼账号自动回复买家消息
- 拍下自动发货（卡券/网盘链接等虚拟商品）
- 批量发布/管理商品
- **AI 客服自动接待**，人不在也能答

xianyu-lite 就是为此裁剪的单机版：**解压 → 装依赖 → 一键启动**，数据库用 SQLite 单文件，缓存用内置兼容层，全程零外部服务。

## 功能一览

| 功能 | 说明 |
|---|---|
| 📊 仪表盘 | 账号在线状态、消息量、订单概览 |
| 👤 账号管理 | 扫码登录闲鱼、多账号切换、监听重启 |
| 💬 在线聊天 | 实时聊天窗口 + **AI 客服自动回复** |
| 🛒 商品管理 | 在售商品列表、价格/库存查看 |
| 🎫 卡券管理 | 虚拟商品卡券导入、**拍下自动发货** |
| 📦 订单管理 | 订单同步与查询 |
| 🚀 商品发布 | 单品/批量发布、素材库、发布日志 |
| ⚙️ 系统设置 | 管理员、定时任务 |

### AI 客服

支持任何 **OpenAI 兼容接口**：DeepSeek、通义千问、Kimi、Gemini（经 OpenAI 兼容网关）、本地 Ollama 等都能接。在「账号管理 → AI 设置」填入 `base_url + api_key + model` 即用，可自定义人设和回复风格。

## 快速开始

环境要求：Python 3.12+、Node 18+

```bash
git clone https://github.com/xiamingyang70/xianyu-lite.git
cd xianyu-lite

# 后端依赖
python -m venv .venv && source .venv/bin/activate
pip install -r backend-web/requirements.txt
pip install aiosqlite fakeredis

# 前端构建
cd frontend && npm install && npx vite build && cd ..

# 一键启动
bash start.sh
```

- 前端：http://localhost:9010
- 后端 API：http://localhost:8099
- 默认管理员：`admin`（首次登录后请立即修改密码）
- 首次启动自动在 `data/xianyu_lite.db` 建 73 张表并完成初始化

## 与原版的差异

| | 原版 xianyu-auto-reply | xianyu-lite |
|---|---|---|
| 数据库 | MySQL 8（需安装） | **SQLite 单文件**（零安装） |
| 缓存/锁 | Redis（需安装） | **内置兼容层**（零安装） |
| 菜单 | 17 主菜单 + 15 管理项 | **8 主菜单 + 2 管理项** |
| 定位 | 规模化运营 | **个人 / 小团队开箱即用** |
| 上游功能 | 全功能 | 裁剪掉：搜索采集、批量上架、数据分析、分销、商品监控、多租户管理等 |

裁剪掉的功能只是从界面上收起，代码主体仍在仓库内，需要时可自行开启。

## 常见问题

**Q: 能从原版迁移数据吗？**
A: 表结构与原版一致（73 张表），可用工具从 MySQL 导出后导入 SQLite。

**Q: 一直开机挂机，SQLite 会坏吗？**
A: 正常使用没问题；建议定期备份 `data/xianyu_lite.db` 单个文件即可。

**Q: 支持部署到云服务器吗？**
A: 支持。任意 1 核 1G 以上轻量服务器即可，无公网 IP 也可以配合内网穿透使用。

## 开源协议

AGPL-3.0（与上游 [zhinianboke/xianyu-auto-reply](https://github.com/zhinianboke/xianyu-auto-reply) 一致）。任何基于本项目的修改版必须同样以 AGPL-3.0 开源。

## 致谢

- [zhinianboke/xianyu-auto-reply](https://github.com/zhinianboke/xianyu-auto-reply) — 上游项目
- [cv-cat/XianYuApis](https://github.com/cv-cat/XianYuApis) — 闲鱼 API 技术参考
