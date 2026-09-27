import asyncio
import collections
import json
import os
import random
import time
from array import array

from aiohttp import ClientTimeout, TCPConnector, ClientSession, web

TARGET = os.environ.get("TARGET_URL", "https://hualuopd.com/")
DEFAULT_CONC = int(os.environ.get("CONCURRENCY", "300"))
DEFAULT_DUR = int(os.environ.get("DURATION", "60"))

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
]


def random_headers():
    return {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


class Stats:
    def __init__(self):
        self.success = 0
        self.fail = 0
        self.codes = collections.Counter()
        self.lat = array("d")
        self.qps_peak = 0.0
        self._ts = []
        self._start = time.perf_counter()

    def record(self, ok, code, elapsed):
        self.codes[code] += 1
        if ok:
            self.success += 1
            self.lat.append(elapsed)
        else:
            self.fail += 1

    def snapshot(self):
        lat = sorted(self.lat)
        n = len(lat)
        total = self.success + self.fail
        dur = max(time.perf_counter() - self._start, 1e-6)
        avg_qps = total / dur

        def pct(p):
            return lat[min(int(p * n), n - 1)] if lat else 0.0

        return {
            "total": total,
            "success": self.success,
            "fail": self.fail,
            "success_rate": round(self.success / max(total, 1) * 100, 2),
            "avg_qps": round(avg_qps, 2),
            "avg_resp": round(sum(lat) / n, 3) if lat else 0,
            "p50": round(pct(0.50), 3),
            "p90": round(pct(0.90), 3),
            "p99": round(pct(0.99), 3),
            "codes": {str(k): v for k, v in self.codes.most_common(10)},
        }


async def hammer(session, sem, stats, stop, url, conc, dur):
    timeout = ClientTimeout(total=10, sock_connect=10, sock_read=10)
    deadline = time.perf_counter() + dur
    while not stop.is_set() and time.perf_counter() < deadline:
        async with sem:
            try:
                t0 = time.perf_counter()
                async with session.get(url, headers=random_headers(), timeout=timeout) as resp:
                    await resp.read()
                    stats.record(resp.status == 200, resp.status, time.perf_counter() - t0)
            except Exception:
                stats.record(False, "ERR", 0)


async def run_pressure(url, conc, dur):
    stats = Stats()
    connector = TCPConnector(limit=conc, limit_per_host=conc, ttl_dns_cache=600, enable_cleanup_closed=True)
    async with ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(conc)
        stop = asyncio.Event()
        tasks = [asyncio.create_task(hammer(session, sem, stats, stop, url, conc, dur))
                 for _ in range(conc)]
        await asyncio.sleep(dur)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    report = stats.snapshot()
    report["target"] = url
    report["concurrency"] = conc
    report["duration"] = dur
    return report


STATE = {"running": False, "report": None, "started_at": None}


def fmt_report(r):
    lines = [
        "======= 压测报告 =======",
        f"目标地址   : {r['target']}",
        f"并发/时长  : {r['concurrency']} 并发 × {r['duration']} 秒",
        f"完成请求   : {r['total']:,}",
        f"成功请求   : {r['success']:,}",
        f"失败请求   : {r['fail']:,}",
        f"成功率     : {r['success_rate']}%",
        f"平均 QPS   : {r['avg_qps']}",
        f"平均响应   : {r['avg_resp']}s",
        f"P50 / P90 / P99: {r['p50']}s / {r['p90']}s / {r['p99']}s",
        f"状态码分布 : " + ", ".join(f"{k}×{v}" for k, v in r["codes"].items()),
        "=======================",
    ]
    return "\n".join(lines)


async def index(request):
    if STATE["running"]:
        return web.Response(text="压测进行中, 访问 /report 查看进度")
    if STATE["report"]:
        return web.Response(text="ok. 上次压测: " + str(STATE["report"]["total"]) + " 请求, 成功率 " + str(STATE["report"]["success_rate"]) + "%")
    return web.Response(text="ok")


async def start(request):
    if STATE["running"]:
        return web.Response(text="已在压测中, 别重复触发")
    url = request.query.get("url", TARGET)
    conc = int(request.query.get("c", DEFAULT_CONC))
    dur = int(request.query.get("d", DEFAULT_DUR))
    conc = max(10, min(conc, 800))
    dur = max(5, min(dur, 600))
    STATE["running"] = True
    STATE["report"] = None
    STATE["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    async def job():
        try:
            STATE["report"] = await run_pressure(url, conc, dur)
        finally:
            STATE["running"] = False

    asyncio.create_task(job())
    return web.Response(text=f"压测已启动: {conc} 并发 × {dur} 秒 → {url}\n完成后访问 /report 查看结果")


async def report(request):
    if STATE["running"]:
        return web.Response(text="压测还在进行中, 稍后再来看 /report")
    if not STATE["report"]:
        return web.Response(text="还没跑过压测, 先访问 /start 触发")
    fmt = request.query.get("fmt", "text")
    if fmt == "json":
        return web.json_response(STATE["report"])
    return web.Response(text=fmt_report(STATE["report"]), content_type="text/plain", charset="utf-8")


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/start", start)
app.router.add_get("/report", report)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, port=port, print=lambda *a, **k: None)
