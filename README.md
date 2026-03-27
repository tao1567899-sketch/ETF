# 📊 国内ETF智能分析系统

> 基于 **AkShare** 数据 + **MiniMax LLM** 智能分析 + **GitHub Actions** 定时运行
> 支持：行情抓取 · 技术分析 · AI报告 · 多渠道推送

---

## ✨ 功能特性

| 功能 | 说明 |
|------|------|
| 📈 行情抓取 | 基于 AkShare，免费获取 A 股所有 ETF 实时行情 + 历史 K 线 |
| 🔬 技术分析 | MA、RSI、MACD、布林带、KDJ、量比，综合评分 -100~+100 |
| 🤖 LLM 智能报告 | 调用 MiniMax API，生成专业行情解读 + 操作建议 |
| 📅 GitHub Actions | 周一至周五 16:15 自动运行，报告保存为 Artifact |
| 📣 多渠道推送 | 企业微信 / 钉钉 / Telegram / 邮件 |
| 🌐 Web 仪表盘 | FastAPI + 内置前端，可本地运行查看分析结果 |

---

## 🚀 快速开始

### 方式一：GitHub Actions（推荐）

#### 1. Fork 本仓库

点击右上角 **Fork** 按钮。

#### 2. 配置 Secrets

进入仓库 **Settings → Secrets and variables → Actions → New repository secret**，添加以下密钥：

**必填：**
| Secret 名称 | 说明 | 示例 |
|------------|------|------|
| `LLM_API_KEY` | MiniMax API Key | `eyJhbGciOi...` |
| `MINIMAX_GROUP_ID` | MiniMax Group ID | `1234567890` |

**推送渠道（至少配置一个）：**
| Secret 名称 | 说明 |
|------------|------|
| `WECOM_WEBHOOK` | 企业微信 Webhook URL |
| `DINGTALK_WEBHOOK` | 钉钉机器人 Webhook URL |
| `DINGTALK_SECRET` | 钉钉机器人加签密钥 |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token |
| `TELEGRAM_CHAT_ID` | Telegram Chat ID |
| `SMTP_HOST` / `SMTP_USER` / `SMTP_PASS` / `SMTP_TO` | 邮件配置 |

**自定义 ETF 列表（可选）：**
| Secret 名称 | 说明 | 示例 |
|------------|------|------|
| `ETF_LIST` | 要分析的 ETF 代码（逗号分隔）| `510050,510300,159915` |
| `NOTIFY_CHANNELS` | 启用的推送渠道 | `wecom,dingtalk` |

#### 3. 触发运行

- **自动运行**：每个交易日 16:15（北京时间）自动执行
- **手动运行**：Actions → `📊 每日ETF智能分析` → Run workflow
- **手动指定ETF**：Actions → `🔍 手动分析ETF` → 输入代码 → Run workflow

#### 4. 查看报告

Actions → 点击最新的 workflow run → Artifacts → 下载 `etf-report-xxx`

---

### 方式二：本地运行

```bash
# 1. 克隆项目
git clone https://github.com/your-username/etf-analysis.git
cd etf-analysis

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置环境变量
cp .env.example .env
# 编辑 .env，填写 LLM_API_KEY 等配置

# 4. 查看可用 ETF 列表
python main.py list

# 5. 分析指定 ETF（控制台输出）
python main.py analyze --codes 510050 510300 159915

# 6. 分析 + 保存报告 + 推送通知
python main.py analyze --output all

# 7. 启动 Web 仪表盘（访问 http://localhost:8080）
python main.py server

# 8. 启动定时任务（本地版）
python main.py schedule
```

---

### 方式三：Docker

```bash
cp .env.example .env  # 编辑 .env

# 启动 Web 仪表盘
docker compose -f docker/docker-compose.yml up web

# 同时启动定时任务
docker compose -f docker/docker-compose.yml --profile scheduler up
```

---

## 📁 项目结构

```
etf-analysis/
├── main.py                        # 程序入口
├── requirements.txt               # Python 依赖
├── .env.example                   # 环境变量模板
├── .gitignore
│
├── .github/
│   └── workflows/
│       ├── daily_analysis.yml     # 每日定时分析（核心）
│       └── manual_analyze.yml     # 手动触发分析
│
├── src/
│   ├── config.py                  # 配置管理
│   ├── analyzer.py                # 分析协调器（核心）
│   ├── technical.py               # 技术指标计算
│   ├── llm_analyzer.py            # LLM 智能分析
│   ├── models.py                  # 数据模型 & 报告格式
│   ├── reporter.py                # Markdown 报告生成
│   └── logger.py                  # 日志配置
│
├── data_provider/
│   ├── akshare_provider.py        # AkShare 数据获取
│   └── etf_registry.py            # ETF 注册表（名称/指数/类型）
│
├── notifier/
│   └── dispatcher.py              # 多渠道推送分发器
│
├── scheduler/
│   └── cron.py                    # 定时任务（本地版）
│
├── web/
│   └── app.py                     # FastAPI Web 服务
│
├── reports/                       # 生成的分析报告（.md）
└── docker/
    ├── Dockerfile
    └── docker-compose.yml
```

---

## 📊 技术指标说明

| 指标 | 信号逻辑 | 权重 |
|------|---------|------|
| **均线 MA** | 均线多头排列 → +20，空头 → -20 | 20 |
| **RSI(14)** | >70 超买 → -15，<30 超卖 → +15 | 15 |
| **MACD** | 金叉 → +25，死叉 → -25 | 25 |
| **KDJ** | 金叉 → +20，死叉 → -20，J<10 超卖 → +10 | 20 |
| **布林带** | 触下轨 → +10，触上轨 → -10 | 10 |
| **量比** | 放量上涨 → +10，缩量 → -5 | 10 |

综合评分区间：**-100 ~ +100**
- 🚀 强烈看多：≥ +50
- 📈 看多：+20 ~ +50
- ➡️ 中性：-20 ~ +20
- 📉 看空：-20 ~ -50
- 🔴 强烈看空：≤ -50

---

## 🔑 获取 MiniMax API Key

1. 访问 [MiniMax 开放平台](https://api.minimax.chat/)
2. 注册 / 登录账号
3. 进入「账号信息」获取 **API Key** 和 **Group ID**
4. 填入 GitHub Secrets 或 `.env` 文件

---

## ⚠️ 免责声明

本项目仅供技术学习和研究使用，所有分析结果（包括 AI 生成报告）**不构成投资建议**。ETF 投资存在风险，请投资者独立判断，自行承担投资风险。
