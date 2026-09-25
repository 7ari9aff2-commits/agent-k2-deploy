"""Owner dashboard — every LLM call with tokens, cost, and latency (like the n8n
executions view, but for the FastAPI core).

GET /dashboard?token=<K2_INTERNAL_TOKEN>            → the HTML page (self-contained, RTL)
GET /dashboard/api/rows?token=...&days=7            → JSON rows (newest first)
Auth: the same K2 internal token, via query string so the browser can use it.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.core.config import settings

router = APIRouter(tags=["Dashboard"])


def _token_ok(request: Request, token: Optional[str]) -> bool:
    import hmac as _hmac
    given = token or request.query_params.get("token") or ""
    return bool(given) and _hmac.compare_digest(str(given), settings.K2_INTERNAL_TOKEN)


async def _rows(days: int, clinic_id: Optional[str], limit: int) -> list:
    from app.db.pool import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        return [dict(r) for r in await conn.fetch(
            """SELECT a.created_at, a.clinic_id, c.name AS clinic_name,
                      a.conversation_id, a.provider, a.model,
                      a.metadata->>'model_node' AS node,
                      a.input_tokens, a.output_tokens, a.total_tokens,
                      a.cost, a.latency_ms
               FROM ai_requests a
               LEFT JOIN clinics c ON c.id = a.clinic_id
               WHERE a.created_at >= now() - ($1::text || ' days')::interval
                 AND ($2::uuid IS NULL OR a.clinic_id = $2::uuid)
               ORDER BY a.created_at DESC
               LIMIT $3""",
            str(days), clinic_id, limit)]


def _totals(rows: list) -> Dict[str, Any]:
    return {
        "calls": len(rows),
        "input_tokens": sum(r.get("input_tokens") or 0 for r in rows),
        "output_tokens": sum(r.get("output_tokens") or 0 for r in rows),
        "total_tokens": sum(r.get("total_tokens") or 0 for r in rows),
        "cost": round(sum(float(r.get("cost") or 0) for r in rows), 6),
        "avg_latency_ms": (round(sum(r.get("latency_ms") or 0 for r in rows
                                     if r.get("latency_ms") is not None)
                                 / max(1, sum(1 for r in rows if r.get("latency_ms") is not None)), 0)
                           if rows else 0),
    }


@router.get("/dashboard/api/rows")
async def dashboard_rows(request: Request, token: str = "", days: int = 7,
                         clinic_id: str = "", limit: int = 300):
    if not _token_ok(request, token):
        return JSONResponse(status_code=403, content={"error": "unauthorized"})
    days = max(1, min(days, 90))
    limit = max(10, min(limit, 1000))
    rows = await _rows(days, clinic_id or None, limit)
    return {"rows": rows, "totals": _totals(rows)}


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, token: str = ""):
    if not _token_ok(request, token):
        return HTMLResponse("<h3>403 — ضع التوكن في الرابط: ?token=...</h3>", status_code=403)
    page = _HTML.replace("__TOKEN__", token)
    return HTMLResponse(page)


_HTML = """<!DOCTYPE html>
<html lang="ar" dir="rtl"><head><meta charset="utf-8">
<title>K2 — استهلاك النموذج</title>
<style>
 body{font-family:system-ui,Segoe UI,Tahoma;background:#0f1420;color:#e8ecf4;margin:0;padding:24px}
 h1{font-size:20px;margin:0 0 4px} .sub{color:#8b95a8;font-size:13px;margin-bottom:16px}
 .cards{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px}
 .card{background:#1a2233;border:1px solid #2a3550;border-radius:10px;padding:12px 18px;min-width:130px}
 .card .v{font-size:22px;font-weight:700;color:#7fd1ff}
 .card .l{font-size:12px;color:#8b95a8}
 table{width:100%;border-collapse:collapse;font-size:13px;background:#141b2b;border-radius:10px;overflow:hidden}
 th{background:#1a2233;color:#9fb0cc;text-align:right;padding:8px 10px;position:sticky;top:0}
 td{padding:7px 10px;border-top:1px solid #232e47;color:#cdd7e8}
 tr:hover td{background:#1a2438}
 .num{direction:ltr;text-align:left;font-variant-numeric:tabular-nums}
 .lat{color:#9fe6a0}.lat.slow{color:#ffc46b}.lat.vslow{color:#ff8a8a}
 select,input{background:#1a2233;color:#e8ecf4;border:1px solid #2a3550;border-radius:6px;padding:6px 10px}
 .bar{display:flex;gap:10px;align-items:center;margin-bottom:14px;flex-wrap:wrap}
</style></head><body>
<h1>K2 — استهلاك النموذج لكل عيادة</h1>
<div class="sub">كل صف = نداء نموذج واحد داخل رسالة (الوكيل أو الكومبوزر). التحديث تلقائي كل 30 ثانية.</div>
<div class="bar">
 <label>العيادة <select id="clinic"><option value="">الكل</option></select></label>
 <label>المدة <select id="days"><option value="1">24 ساعة</option><option value="7" selected>7 أيام</option><option value="30">30 يوم</option></select></label>
 <button onclick="load()" style="background:#2a6df5;color:#fff;border:0;border-radius:6px;padding:7px 16px;cursor:pointer">تحديث</button>
</div>
<div class="cards">
 <div class="card"><div class="v" id="t-calls">—</div><div class="l">نداءات</div></div>
 <div class="card"><div class="v" id="t-in">—</div><div class="l">توكنات إدخال</div></div>
 <div class="card"><div class="v" id="t-out">—</div><div class="l">توكنات إخراج</div></div>
 <div class="card"><div class="v" id="t-cost">—</div><div class="l">التكلفة (USD)</div></div>
 <div class="card"><div class="v" id="t-lat">—</div><div class="l">متوسط الزمن (ms)</div></div>
</div>
<table><thead><tr>
 <th>الوقت</th><th>العيادة</th><th>المحادثة</th><th>النموذج</th><th>الدور</th>
 <th>إدخال</th><th>إخراج</th><th>الإجمالي</th><th>التكلفة</th><th>الزمن ms</th>
</tr></thead><tbody id="rows"></tbody></table>
<script>
const TOKEN = location.search.get('token') || '';
const API = '/dashboard/api/rows?token=' + encodeURIComponent(TOKEN);
let CLINICS_LOADED = false;
function fmt(n){return n===null||n===undefined?'—':Number(n).toLocaleString('en-US');}
function latCls(v){if(v===null||v===undefined)return 'lat';if(v>15000)return 'lat vslow';if(v>5000)return 'lat slow';return 'lat';}
async function load(){
 const days = document.getElementById('days').value;
 const clinic = document.getElementById('clinic').value;
 const r = await fetch(API + '&days=' + days + '&clinic_id=' + clinic);
 if(r.status !== 200){document.getElementById('rows').innerHTML = '<tr><td>403 — توكن غير صالح</td></tr>';return;}
 const d = await r.json();
 document.getElementById('t-calls').textContent = fmt(d.totals.calls);
 document.getElementById('t-in').textContent = fmt(d.totals.input_tokens);
 document.getElementById('t-out').textContent = fmt(d.totals.output_tokens);
 document.getElementById('t-cost').textContent = d.totals.cost.toFixed(4);
 document.getElementById('t-lat').textContent = fmt(d.totals.avg_latency_ms);
 const seen = new Set();
 for(const row of d.rows){ if(row.clinic_name && !seen.has(row.clinic_id)){seen.add(row.clinic_id);
   const o = document.createElement('option'); o.value = row.clinic_id; o.textContent = row.clinic_name;
   document.getElementById('clinic').appendChild(o);} }
 if(!CLINICS_LOADED){CLINICS_LOADED = true;
   for(const o of [...document.getElementById('clinic').options]) if(o.value && !seen.has(o.value)) o.remove();}
 document.getElementById('rows').innerHTML = d.rows.map(r => {
  const t = new Date(r.created_at);
  const lat = r.latency_ms;
  return '<tr><td class="num">' + t.toLocaleString('en-GB',{hour12:false}) + '</td>'
   + '<td>' + (r.clinic_name || r.clinic_id.slice(0,8)) + '</td>'
   + '<td class="num" title="' + r.conversation_id + '">' + r.conversation_id.slice(0,8) + '</td>'
   + '<td class="num">' + (r.model || '').split('/').pop() + '</td>'
   + '<td>' + (r.node || '—') + '</td>'
   + '<td class="num">' + fmt(r.input_tokens) + '</td>'
   + '<td class="num">' + fmt(r.output_tokens) + '</td>'
   + '<td class="num">' + fmt(r.total_tokens) + '</td>'
   + '<td class="num">' + (r.cost === null ? '—' : Number(r.cost).toFixed(5)) + '</td>'
   + '<td class="num ' + latCls(lat) + '">' + (lat === null || lat === undefined ? '—' : fmt(lat)) + '</td></tr>';
 }).join('');
}
document.getElementById('clinic').onchange = load;
document.getElementById('days').onchange = load;
load();
setInterval(load, 30000);
</script></body></html>"""
