"""
FastAPI Web 服务
提供 REST API 接口和 Web 仪表盘
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from src.config import config
from src.logger import logger

app = FastAPI(
    title="国内ETF智能分析系统",
    description="基于LLM的国内ETF技术分析与智能报告平台",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 内存缓存（生产环境可替换为 Redis）
_cache: dict = {}
_analysis_tasks: dict = {}


# ── 数据模型 ──────────────────────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    codes: List[str]
    use_llm: bool = True


class AnalyzeResponse(BaseModel):
    task_id: str
    status: str
    message: str


# ── API 路由 ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    """Web 仪表盘首页"""
    html_path = Path(__file__).parent / "dashboard.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text(encoding="utf-8"))
    return HTMLResponse(INLINE_DASHBOARD_HTML)


@app.get("/api/v1/etf/list")
async def list_etfs():
    """获取ETF注册列表"""
    from data_provider.etf_registry import ETF_REGISTRY
    return {
        "total": len(ETF_REGISTRY),
        "etfs": [
            {"code": code, **info}
            for code, info in ETF_REGISTRY.items()
        ],
    }


@app.get("/api/v1/etf/{code}/quote")
async def get_etf_quote(code: str):
    """获取单只ETF实时行情"""
    from data_provider.akshare_provider import AkShareProvider
    provider = AkShareProvider()
    quote = await provider.get_realtime_quote(code)
    if not quote:
        raise HTTPException(status_code=404, detail=f"ETF {code} 行情数据不可用")
    return quote


@app.get("/api/v1/etf/{code}/analyze")
async def analyze_etf(code: str, use_llm: bool = Query(default=True)):
    """分析单只ETF（同步，有缓存）"""
    cache_key = f"analyze_{code}"
    cached = _cache.get(cache_key)
    if cached:
        logger.info(f"返回缓存结果: {code}")
        return cached

    from src.analyzer import ETFAnalyzer
    analyzer = ETFAnalyzer()
    result = await analyzer.analyze_single(code)
    data = result.to_dict()
    _cache[cache_key] = data
    return data


@app.post("/api/v1/etf/batch-analyze")
async def batch_analyze(request: AnalyzeRequest, background_tasks: BackgroundTasks):
    """批量分析ETF（异步任务）"""
    task_id = f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(request.codes)}"
    _analysis_tasks[task_id] = {"status": "pending", "results": None}

    async def run_task():
        try:
            _analysis_tasks[task_id]["status"] = "running"
            from src.analyzer import ETFAnalyzer
            analyzer = ETFAnalyzer()
            results = await analyzer.analyze_batch(request.codes)
            _analysis_tasks[task_id]["status"] = "done"
            _analysis_tasks[task_id]["results"] = [r.to_dict() for r in results]
        except Exception as e:
            _analysis_tasks[task_id]["status"] = "error"
            _analysis_tasks[task_id]["error"] = str(e)

    background_tasks.add_task(run_task)
    return AnalyzeResponse(
        task_id=task_id,
        status="pending",
        message=f"任务已创建，正在分析 {len(request.codes)} 只ETF",
    )


@app.get("/api/v1/tasks/{task_id}")
async def get_task_status(task_id: str):
    """查询异步任务状态"""
    task = _analysis_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    return task


@app.get("/api/v1/market/overview")
async def get_market_overview():
    """获取大盘指数概览"""
    from data_provider.akshare_provider import AkShareProvider
    provider = AkShareProvider()
    data = await provider.get_market_overview()
    return {"indices": data, "timestamp": datetime.now().isoformat()}


@app.get("/api/v1/config/etf-list")
async def get_config_etf_list():
    """获取当前配置的ETF列表"""
    return {"etf_list": config.etf_list}


@app.get("/api/v1/reports")
async def list_reports():
    """列出历史分析报告"""
    report_dir = Path(config.report_dir)
    if not report_dir.exists():
        return {"reports": []}
    reports = sorted(report_dir.glob("etf_analysis_*.md"), reverse=True)
    return {
        "reports": [
            {
                "filename": r.name,
                "size": r.stat().st_size,
                "created_at": datetime.fromtimestamp(r.stat().st_mtime).isoformat(),
            }
            for r in reports[:20]
        ]
    }


@app.get("/api/v1/health")
async def health_check():
    """健康检查"""
    return {
        "status": "ok",
        "version": "1.0.0",
        "llm_configured": bool(config.llm_api_key),
        "etf_count": len(config.etf_list),
        "timestamp": datetime.now().isoformat(),
    }


# ── 内联仪表盘 HTML（fallback） ───────────────────────────────────────────────

INLINE_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>国内ETF智能分析系统</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'PingFang SC', sans-serif;
         background: #0f1117; color: #e2e8f0; min-height: 100vh; }
  .header { background: linear-gradient(135deg, #1a1f2e, #2d3748);
            padding: 20px 32px; border-bottom: 1px solid #2d3748; }
  .header h1 { font-size: 1.5rem; color: #63b3ed; }
  .header p { color: #718096; font-size: 0.875rem; margin-top: 4px; }
  .container { max-width: 1200px; margin: 0 auto; padding: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 16px; }
  .card { background: #1a1f2e; border: 1px solid #2d3748; border-radius: 12px; padding: 20px; }
  .card h3 { color: #63b3ed; margin-bottom: 12px; font-size: 0.95rem; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
  .badge-buy { background: rgba(72,187,120,0.2); color: #68d391; }
  .badge-sell { background: rgba(252,129,74,0.2); color: #fc8d4a; }
  .badge-neutral { background: rgba(113,128,150,0.2); color: #a0aec0; }
  .price { font-size: 1.5rem; font-weight: 700; color: #e2e8f0; }
  .change-up { color: #f56565; }
  .change-down { color: #48bb78; }
  .indicator-row { display: flex; justify-content: space-between; padding: 4px 0;
                   border-bottom: 1px solid #2d3748; font-size: 0.8rem; }
  .indicator-row:last-child { border-bottom: none; }
  .indicator-label { color: #718096; }
  .indicator-value { color: #e2e8f0; font-weight: 500; }
  .btn { background: #3182ce; color: white; border: none; padding: 8px 16px;
         border-radius: 6px; cursor: pointer; font-size: 0.875rem; }
  .btn:hover { background: #2b6cb0; }
  .input-row { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
  input[type=text] { background: #2d3748; border: 1px solid #4a5568; color: #e2e8f0;
                     padding: 8px 12px; border-radius: 6px; font-size: 0.875rem; flex: 1; }
  .loading { text-align: center; color: #718096; padding: 40px; }
  .llm-box { background: #111827; border-radius: 8px; padding: 12px; margin-top: 12px;
             font-size: 0.8rem; line-height: 1.6; color: #a0aec0; white-space: pre-wrap; }
  .score-bar { height: 4px; background: #2d3748; border-radius: 2px; margin-top: 8px; }
  .score-fill { height: 100%; border-radius: 2px; transition: width 0.5s; }
</style>
</head>
<body>
<div class="header">
  <h1>📊 国内ETF智能分析系统</h1>
  <p>基于 AkShare 数据 + LLM 智能分析 | 实时行情 · 技术指标 · AI报告</p>
</div>
<div class="container">
  <div style="margin-bottom:24px">
    <div class="input-row">
      <input type="text" id="codeInput" placeholder="输入ETF代码，多个用逗号分隔，如: 510050,510300,159915" />
      <button class="btn" onclick="analyzeETFs()">🔍 分析</button>
      <button class="btn" style="background:#2d3748" onclick="loadDefaultList()">📋 默认列表</button>
    </div>
  </div>
  <div id="marketOverview" style="margin-bottom:24px"></div>
  <div id="results" class="grid"></div>
</div>

<script>
const API = '';
let analysisData = [];

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function loadMarketOverview() {
  try {
    const data = await fetchJSON('/api/v1/market/overview');
    const div = document.getElementById('marketOverview');
    const indices = data.indices || {};
    const items = Object.entries(indices).map(([name, info]) => {
      const cls = info.change_pct >= 0 ? 'change-up' : 'change-down';
      const sign = info.change_pct >= 0 ? '+' : '';
      return `<div style="background:#1a1f2e;border:1px solid #2d3748;border-radius:8px;padding:12px 16px;text-align:center">
        <div style="color:#718096;font-size:0.75rem">${name}</div>
        <div style="font-size:1.1rem;font-weight:700;margin:4px 0">${info.price.toFixed(2)}</div>
        <div class="${cls}">${sign}${info.change_pct.toFixed(2)}%</div>
      </div>`;
    }).join('');
    div.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:12px;margin-bottom:16px">${items}</div>`;
  } catch(e) { console.warn('大盘数据获取失败', e); }
}

async function loadDefaultList() {
  try {
    const data = await fetchJSON('/api/v1/config/etf-list');
    document.getElementById('codeInput').value = data.etf_list.join(',');
  } catch(e) { alert('获取默认列表失败'); }
}

async function analyzeETFs() {
  const input = document.getElementById('codeInput').value.trim();
  if (!input) { alert('请输入ETF代码'); return; }
  const codes = input.split(/[,，\s]+/).filter(c => c.trim());

  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = '<div class="loading">🔄 正在分析中，请稍候（首次加载可能需要30-60秒）...</div>';

  try {
    // 逐个分析（并发请求）
    const promises = codes.map(code =>
      fetchJSON(`/api/v1/etf/${code.trim()}/analyze`).catch(e => ({error: e.message, code}))
    );
    const results = await Promise.all(promises);
    renderResults(results);
  } catch(e) {
    resultsDiv.innerHTML = `<div class="loading" style="color:#f56565">❌ 分析失败: ${e.message}</div>`;
  }
}

function getSignalBadge(signal) {
  const map = {
    strong_buy: ['badge-buy', '🚀 强烈看多'],
    buy: ['badge-buy', '📈 看多'],
    neutral: ['badge-neutral', '➡️ 中性'],
    sell: ['badge-sell', '📉 看空'],
    strong_sell: ['badge-sell', '🔴 强烈看空'],
  };
  const [cls, text] = map[signal] || ['badge-neutral', '❓'];
  return `<span class="badge ${cls}">${text}</span>`;
}

function renderResults(results) {
  const div = document.getElementById('results');
  if (!results.length) { div.innerHTML = '<div class="loading">无结果</div>'; return; }

  div.innerHTML = results.map(r => {
    if (r.error && !r.code) return `<div class="card"><p style="color:#f56565">❌ ${r.error}</p></div>`;
    const q = r.quote || {};
    const t = r.technical || {};
    const changeCls = q.change_pct >= 0 ? 'change-up' : 'change-down';
    const changeSign = q.change_pct >= 0 ? '+' : '';
    const score = t.signal_score || 0;
    const scoreColor = score > 30 ? '#48bb78' : score < -30 ? '#f56565' : '#ed8936';
    const scoreWidth = Math.min(100, Math.abs(score));
    const scoreLeft = score >= 0 ? 50 : 50 - scoreWidth/2;

    const indicators = [
      ['MA5/20', `${t.ma5 || 'N/A'} / ${t.ma20 || 'N/A'}`, t.ma_trend],
      ['RSI(14)', t.rsi14 || 'N/A', t.rsi_signal],
      ['MACD', `${t.macd || 'N/A'}`, t.macd_cross !== 'none' ? t.macd_cross : ''],
      ['KDJ K/D', `${t.kdj_k || 'N/A'} / ${t.kdj_d || 'N/A'}`, t.kdj_cross !== 'none' ? t.kdj_cross : ''],
      ['量比', t.volume_ratio || 'N/A', t.volume_trend],
    ].map(([label, val, note]) =>
      `<div class="indicator-row">
        <span class="indicator-label">${label}</span>
        <span class="indicator-value">${val} ${note ? `<span style="color:#718096;font-size:0.7rem">[${note}]</span>` : ''}</span>
      </div>`
    ).join('');

    const signalsList = (t.signals || []).slice(0,3).map(s =>
      `<div style="font-size:0.75rem;color:#a0aec0;padding:2px 0">${s}</div>`
    ).join('');

    const llmHtml = r.llm_analysis
      ? `<div class="llm-box">🤖 ${r.llm_analysis}</div>`
      : '';

    return `<div class="card">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px">
        <div>
          <div style="font-weight:600;font-size:1rem">${r.name || r.code}</div>
          <div style="color:#718096;font-size:0.75rem">${r.code} · ${r.index || ''}</div>
        </div>
        ${getSignalBadge(t.overall_signal)}
      </div>
      <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:4px">
        <span class="price">${(q.price || 0).toFixed(4)}</span>
        <span class="${changeCls}">${changeSign}${(q.change_pct || 0).toFixed(2)}%</span>
      </div>
      <div style="font-size:0.75rem;color:#718096;margin-bottom:12px">
        成交: ${((q.turnover||0)/1e8).toFixed(2)}亿 · 换手: ${(q.turnover_rate||0).toFixed(2)}%
      </div>

      <div style="margin-bottom:12px">
        <div style="font-size:0.75rem;color:#718096;margin-bottom:4px">评分: ${score > 0 ? '+' : ''}${score.toFixed(1)}</div>
        <div class="score-bar">
          <div class="score-fill" style="width:${scoreWidth/2}%;margin-left:${score>=0?50:50-scoreWidth/2}%;background:${scoreColor}"></div>
        </div>
      </div>

      <div style="margin-bottom:8px">${indicators}</div>
      ${signalsList}
      ${llmHtml}
    </div>`;
  }).join('');
}

// 初始化
loadMarketOverview();
loadDefaultList();
</script>
</body>
</html>"""
