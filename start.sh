#!/bin/bash
# xianyu-lite 一键启动: 后端8099(SQLite+fakeredis) + 前端9010
cd "$(dirname "$0")"

# 后端
(cd backend-web && REDIS_MODE=fakeredis PYTHONPATH="$(pwd)/..":"$(pwd)" ../.venv/bin/python main.py >> ../logs/backend.log 2>&1 &)

# 前端 (vite preview 代理需指向8099)
(cd frontend && [ -L node_modules ] || ln -sfn "$(pwd)/../../xianyu-auto-reply/frontend/node_modules" node_modules
 npx vite preview --port 9010 --host 0.0.0.0 >> ../logs/frontend.log 2>&1 &)

echo "lite 启动中: 前端 http://localhost:9010  后端 http://localhost:8099"
