# xianyu-lite

**闲鱼多账号自动化助手 · 精简单机版** — 开箱即用，无需安装 MySQL / Redis。

基于开源项目 [xianyu-auto-reply](https://pan.quark.cn/s/af567356cba7)（AGPL-3.0）裁剪而来，遵循同一协议开源。

## 特点

- **零外部数据库** — SQLite 单文件库（`data/xianyu_lite.db`），首次启动自动建表（73 张表）并初始化
- **零 Redis** — 内置 fakeredis 兼容层，单机部署不再需要装 Redis
- **精简界面** — 只保留高频功能：仪表盘 / 账号管理 / 在线聊天（AI 客服） / 商品管理 / 卡券管理 / 订单管理 / 商品发布 / 个人设置
- **AI 客服内置** — 支持任何 OpenAI 兼容接口（含 Gemini 经代理网关），在「账号管理 → AI 设置」填 key 即用
- **一键启动** — `bash start.sh`

## 快速开始

```bash
# 1. 安装依赖 (Python 3.12+ 与 Node 18+)
python -m venv .venv && source .venv/bin/activate
pip install -r backend-web/requirements.txt   # 见 pyproject.toml
pip install aiosqlite fakeredis
cd frontend && npm install && npx vite build && cd ..

# 2. 启动
bash start.sh
# 前端 http://localhost:9010  后端 http://localhost:8099
# 默认账号 admin (首次登录后请立即改密)
```

Docker 方式（可选）见 `docker-compose.yml`。

## 与原版的差异

| | 原版 xianyu-auto-reply | xianyu-lite |
|---|---|---|
| 数据库 | MySQL 8 | **SQLite 单文件** |
| 缓存/锁 | Redis | **内置 fakeredis** |
| 菜单 | 17 主菜单 + 15 管理项 | **8 主菜单 + 2 管理项** |
| 定位 | 多账号规模化运营 | **单机 / 小团队开箱即用** |

## 开源协议

AGPL-3.0（与上游一致）。任何基于本项目的修改版必须同样以 AGPL-3.0 开源。
