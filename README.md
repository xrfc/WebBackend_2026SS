# WebBackend — AI驱动的高并发秒杀与智能电商后台系统

> 🎓 《Web后端开发技术》期末大作业 · 题目一  
> 6个微服务 | Redis Lua原子扣减 | RabbitMQ异步削峰 | WebSocket实时推送 | DeepSeek AI导购

---

## 🏗 系统架构

```
客户端 → [Gateway :8000] → 路由分发 + JWT鉴权
              │
   ┌──────────┼──────────┬──────────────┬─────────────┐
   ▼          ▼          ▼              ▼             ▼
User(:8001) Product(:8002) Seckill(:8004) Order(:8003) AI(:8005)
   │          │          │              │             │
   └──────────┴──────────┴──────┬───────┘             │
                                │                     │
                          [MySQL :3306]          [DeepSeek V4 Pro]
                          [Redis :6379]
                      [RabbitMQ :5672]
```

| 服务 | 端口 | 职责 |
|------|------|------|
| Gateway | 8000 | API网关：统一入口、JWT鉴权、路由转发 |
| UserService | 8001 | 注册/登录/JWT/角色权限(customer/admin) |
| ProductService | 8002 | 商品CRUD、库存管理、分页搜索 |
| OrderService | 8003 | 订单查询、MQ消费落库、WebSocket推送 |
| SeckillService | 8004 | Redis预热 + Lua原子扣减 + MQ投递 |
| AIService | 8005 | DeepSeek LLM智能商品导购 |

---

## 🚀 快速开始

### 前置条件

- Python 3.12+
- Docker Desktop（用于中间件，可选）

### 1. 克隆项目

```bash
git clone <仓库地址>
cd WebBackend
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3a. 轻量启动（仅测试，无需Docker）

```bash
# 精简版（推荐，~15行关键日志，1秒完成）
python test_integration.py --summary

# 完整版（10000条请求日志）
python test_integration.py
```

### 3b. 完整启动（需要Docker）

```bash
# 一键启动：自动停本地MySQL → 启动Docker中间件 → 启动6个微服务
start_all.bat
```

---

## 📖 API文档

启动后访问 Swagger 文档：

| 服务 | 地址 |
|------|------|
| 网关（总入口） | http://localhost:8000/docs |
| 用户服务 | http://localhost:8001/docs |
| 商品服务 | http://localhost:8002/docs |
| 订单服务 | http://localhost:8003/docs |
| 秒杀服务 | http://localhost:8004/docs |
| AI服务 | http://localhost:8005/docs |

### Apifox / Postman 测试

导入 `postman_collection.json`，按文件夹顺序执行即可。Token和product_id自动传递。

---

## 📁 项目结构

```
WebBackend/
├── common/                        # 公共模块
│   ├── config.py                  # 统一配置（数据库/Redis/MQ/LLM）
│   ├── database.py                # SQLAlchemy异步引擎 + 自动建表
│   ├── auth.py                    # JWT签发/校验 + bcrypt密码 + 角色鉴权
│   ├── response.py                # 统一API响应格式
│   └── test_mq.py                 # 测试模式共享内存MQ
│
├── services/                      # 6个独立微服务
│   ├── gateway/main.py            # API网关（反向代理+JWT鉴权）
│   ├── user_service/main.py       # 用户服务（注册/登录/权限）
│   ├── product_service/main.py    # 商品服务（CRUD/分页/库存）
│   ├── order_service/main.py      # 订单服务（MQ消费+WebSocket推送）
│   ├── seckill_service/main.py    # ⭐ 秒杀服务（Redis+Lua原子扣减）
│   └── ai_service/main.py         # AI服务（DeepSeek LLM导购）
│
├── docker-compose.yml             # MySQL + Redis + RabbitMQ
├── requirements.txt               # Python依赖
├── .env                           # 环境变量配置
├── start_all.bat                  # Windows一键启动脚本
├── postman_collection.json        # API测试集合（可导入Apifox/Postman）
├── test_integration.py            # 一体化验收测试（纯内存，无需Docker）
└── run_and_test.py                # 启动服务+端到端HTTP测试
```

---

## 🧪 测试

### 验收测试

```bash
python test_integration.py --summary
```

**测试结果：**

```
============================================================
  最终验收报告
============================================================
  ✅ PASS  用户注册/登录/JWT鉴权
  ✅ PASS  商品CRUD管理
  ✅ PASS  Redis库存预热
  ✅ PASS  高并发Lua原子扣减          ← 10000并发, 无超卖
  ✅ PASS  0超卖验证                  ← Redis库存=0, 订单=100
  ✅ PASS  MQ异步削峰落库
  ✅ PASS  重复购买防护               ← 拦截9900次无效请求
  ✅ PASS  WebSocket推送端点
  通过: 8/8 | QPS=7980 | 100库存 | 0超卖 | 10000并发
============================================================
```

**请求级日志：**

```
#00000 | 用户   2 → ✅ 扣减成功 | 剩余库存=99
#00001 | 用户   3 → ✅ 扣减成功 | 剩余库存=98
  ...
#00099 | 用户 101 → ✅ 扣减成功 | 剩余库存=0        ← 刚好卖完100件
#00100 | 用户 102 → ❌ 库存不足(已售罄) | 当前库存=0  ← 售罄立即拦截
#00200 | 用户   2 → 🚫 重复购买,已拦截               ← 防重复购买
```

---

## ⚙ 环境变量

`.env` 文件中需配置：

```env
# MySQL（Docker默认）
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=Six666666.
MYSQL_DATABASE=webbackend

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# RabbitMQ
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=admin
RABBITMQ_PASS=admin123

# LLM（DeepSeek V4 Pro）
LLM_API_KEY=sk-your-key-here
LLM_BASE_URL=https://api.deepseek.com
LLM_MODEL=deepseek-v4-pro
```

---

## 🔑 核心技术实现

### 高并发防超卖（Redis + Lua原子脚本）

```lua
-- services/seckill_service/main.py 第55-79行
local stock = redis.call('get', KEYS[1])
if tonumber(stock) <= 0 then return 0 end    -- 库存不足
redis.call('decr', KEYS[1])                   -- 原子扣减
redis.call('sadd', KEYS[2], ARGV[1])           -- 防重复购买
return 1
```

### 异步削峰（RabbitMQ）

Redis扣减成功 → 投递MQ消息 → 立即返回"排队中" → OrderService异步消费落库

### 实时推送（WebSocket）

订单落库后，通过 `ConnectionManager` 向对应user_id的WebSocket连接推送JSON通知。

### AI智能导购（DeepSeek V4 Pro）

获取商品上下文 → 构建System/User Prompt → 调用LLM → 失败时自动降级返回基础信息

---

## 🛠 技术栈

| 层级 | 技术 |
|------|------|
| 语言 | Python 3.12 |
| 框架 | FastAPI 0.115 + Uvicorn 0.30 |
| ORM | SQLAlchemy 2.0（async） + aiomysql |
| 数据库 | MySQL 8.0（Docker） |
| 缓存 | Redis 7（Docker） |
| 消息队列 | RabbitMQ 3（Docker） |
| 认证 | JWT（python-jose） |
| 密码加密 | bcrypt（passlib） |
| AI | DeepSeek V4 Pro（OpenAI兼容API） |
| API文档 | Swagger/OpenAPI（FastAPI自动生成） |

---

## 📄 许可证

本项目为课程作业，仅供学习参考。
