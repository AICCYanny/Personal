# -*- coding: utf-8 -*-
# speed_monitor.py
import time, random, sys, shutil, subprocess, re
from datetime import datetime
from typing import Optional, List
import requests

# ========== 可调参数 ==========
PING_HOSTS = ["1.1.1.1", "8.8.8.8"]  # Cloudflare / Google DNS 等
DEFAULT_INTERVAL = 2.0               # UI 刷新/下载突发的目标间隔（秒）
DEFAULT_BURST = 2_000_000            # 每轮下载突发字节数（~1MB，越小越省流量）
DEFAULT_PING_EVERY_SEC = 10.0        # 每隔多少秒做一次多包 ping（按时间调度）
DEFAULT_PING_COUNT = 5               # 每次 ping 的包数（3~5 较轻）

# ========== 工具函数 ==========
def now_hms() -> str:
    return datetime.now().strftime("%H:%M:%S")

def clearline_write(s: str):
    """单行动态刷新：清行 + 回到行首 + 写入"""
    width = shutil.get_terminal_size((100, 20)).columns
    if len(s) > width:
        s = s[:max(0, width - 1)]
    sys.stdout.write("\r\033[2K" + s)
    sys.stdout.flush()

def _measure_url(url: str, burst_bytes: int, timeout: int) -> Optional[float]:
    """对指定 URL 做轻量突发下载，返回 Mbps；失败返回 None。"""
    try:
        size = 0
        t0 = time.monotonic()
        with requests.get(url, stream=True, timeout=timeout,
                          headers={"User-Agent": "lite-net-monitor/1.0"}) as r:
            r.raise_for_status()
            for chunk in r.iter_content(64 * 1024):
                if not chunk:
                    break
                size += len(chunk)
                if size >= burst_bytes:
                    break
        t1 = time.monotonic()
        if size == 0 or t1 <= t0:
            return None
        return (size * 8 / 1_000_000) / (t1 - t0)  # Mbps
    except Exception:
        return None

def download_mbps(burst_bytes: int = DEFAULT_BURST, timeout: int = 5) -> Optional[float]:
    """优先 Cloudflare 字节流，失败再 Range 源：返回下载 Mbps。"""
    # 1) Cloudflare 字节流端点：稳定、几乎处处可达
    cf_url = f"https://speed.cloudflare.com/__down?bytes={burst_bytes}&nocache={random.randint(1,10_000_000)}"
    mbps = _measure_url(cf_url, burst_bytes, timeout)
    if mbps is not None:
        return mbps

    # 2) 备用 Range 源（只取前 burst_bytes 字节）
    range_urls = [
        "https://speed.hetzner.de/100MB.bin",
        "https://proof.ovh.net/files/100Mb.dat",
        "https://speedtest.tele2.net/100MB.zip",
    ]
    random.shuffle(range_urls)
    for u in range_urls:
        try:
            size = 0
            t0 = time.monotonic()
            with requests.get(
                u, stream=True, timeout=timeout,
                headers={
                    "Range": f"bytes=0-{max(0, burst_bytes - 1)}",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "User-Agent": "lite-net-monitor/1.0",
                },
            ) as r:
                r.raise_for_status()
                for chunk in r.iter_content(64 * 1024):
                    if not chunk:
                        break
                    size += len(chunk)
                    if size >= burst_bytes:
                        break
            t1 = time.monotonic()
            if size == 0 or t1 <= t0:
                continue
            return (size * 8 / 1_000_000) / (t1 - t0)
        except Exception:
            continue
    return None

def ping_stats(host: str, count: int = 5, timeout_s: float = 1.0):
    """
    系统 ping 统计：返回 (min_ms, avg_ms, max_ms, jitter_ms, loss_percent)
    macOS/Linux 兼容：仅用 -c -n；整体超时由 subprocess.run 的 timeout 控制。
    """
    try:
        proc = subprocess.run(
            ["ping", "-c", str(count), "-n", host],
            capture_output=True, text=True,
            timeout=max(timeout_s * count, 2.0),
        )
        out = proc.stdout + proc.stderr
        times = [float(m.group(1)) for m in re.finditer(r"time[=<]\s*([\d\.]+)\s*ms", out)]
        m_loss = re.search(r"(\d+(?:\.\d+)?)%\s*packet loss", out)
        loss = float(m_loss.group(1)) if m_loss else 100.0
        if not times:
            return None, None, None, None, loss
        mn, mx = min(times), max(times)
        avg = sum(times) / len(times)
        jitter = (mx - mn) / 2.0  # 简单抖动估算（极差一半）；也可改成标准差
        return mn, avg, mx, jitter, loss
    except Exception:
        return None, None, None, None, 100.0

# ========== 核心监控 ==========
def monitor(interval: float = DEFAULT_INTERVAL,
            burst_bytes: int = DEFAULT_BURST,
            ping_every_sec: float = DEFAULT_PING_EVERY_SEC,
            ping_count: int = DEFAULT_PING_COUNT):
    """
    interval       : UI 刷新/下载突发的目标间隔（秒）
    burst_bytes    : 每轮下载突发字节数（越小越省流量，500k~2MB 合理）
    ping_every_sec : 每隔多少秒做一次多包 ping（按时间触发，不跟循环计数）
    ping_count     : 每次 ping 发包数（3~5 较轻）
    """
    # 下载统计
    min_down = float("inf")
    max_down = float("-inf")
    # ping 极值统计（会话内）
    best_ping = float("inf")
    worst_ping = float("-inf")
    # 抖动/丢包 极值统计（会话内）
    min_jitter = float("inf")
    max_jitter = float("-inf")
    min_loss = float("inf")
    max_loss = float("-inf")
    # 最近一次 ping 统计（用于行行显示）
    last_avg = last_jitter = last_loss = None
    last_host = None

    start_monotonic = time.monotonic()

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 轻量监控启动（间隔 {interval:.1f}s，突发 ~{burst_bytes/1_000_000:.1f}MB）")
    print("Ctrl+C 结束。\n")

    # 绝对时间片调度
    next_tick = time.monotonic()
    next_ping_due = time.monotonic()  # 按时间调度 ping

    try:
        while True:
            loop_start = time.monotonic()

            # —— 下载估速（设置适度超时）——
            down = download_mbps(burst_bytes=burst_bytes, timeout=max(3, int(burst_bytes / 400_000)))
            if down is not None:
                if down < min_down: min_down = down
                if down > max_down: max_down = down

            # —— 按时间触发多包 ping（统计 avg/jitter/loss）——
            if loop_start >= next_ping_due:
                candidates = []
                for h in PING_HOSTS:
                    mn, avg, mx, jitter, loss = ping_stats(h, count=ping_count, timeout_s=1.0)
                    if avg is not None:
                        candidates.append((avg, mn, mx, jitter, loss, h))
                if candidates:
                    candidates.sort(key=lambda x: x[0])  # 选择平均延迟最低的 host
                    avg, mn, mx, jitter, loss, host = candidates[0]
                    # 极值更新
                    if mn is not None and mn < best_ping: best_ping = mn
                    if mx is not None and mx > worst_ping: worst_ping = mx
                    if jitter is not None:
                        if jitter < min_jitter: min_jitter = jitter
                        if jitter > max_jitter: max_jitter = jitter
                    if loss is not None:
                        if loss < min_loss: min_loss = loss
                        if loss > max_loss: max_loss = loss
                    # 缓存当前显示值
                    last_avg, last_jitter, last_loss, last_host = avg, jitter, loss, host
                # 下一次 ping 的绝对触发时间
                next_ping_due = loop_start + ping_every_sec

            # —— 动态打印（带缓存）——
            down_str = "—" if down is None else f"{down:.2f}"
            fastest = "—" if max_down == float("-inf") else f"{max_down:.2f}"
            slowest = "—" if min_down == float("inf") else f"{min_down:.2f}"
            if last_avg is not None:
                ping_line = f"ping {last_avg:.1f} ms | jitter {last_jitter:.1f} ms | loss {last_loss:.1f}% ({last_host})"
            else:
                ping_line = "ping — ms | jitter — ms | loss — %"
            bestworst = (
                f"{'—' if best_ping == float('inf') else f'{best_ping:.1f}'}/"
                f"{'—' if worst_ping == float('-inf') else f'{worst_ping:.1f}'}"
            )
            line = (
                f"[{now_hms()}] ↓{down_str} Mbps  | 最快/最慢 ↓{fastest}/{slowest}  | "
                f"{ping_line}  | ping 最好/最差 {bestworst}"
            )
            clearline_write(line)

            # —— 绝对对齐到下一时间片 —— 
            next_tick += interval
            now = time.monotonic()
            sleep_s = max(0.0, next_tick - now)
            # 防止单轮异常拖慢：若睡眠超过 2×interval，将节拍拉回
            if sleep_s > 2 * interval:
                next_tick = now + interval
                sleep_s = interval
            time.sleep(sleep_s)

    except KeyboardInterrupt:
        # 结束统计
        runtime = time.monotonic() - start_monotonic
        print("\n\n========== 会话统计 ==========")
        print(f"运行时间：{runtime:.1f} 秒")
        print(f"下载最快: {0 if max_down == float('-inf') else max_down:.2f} Mbps")
        print(f"下载最慢: {0 if min_down == float('inf') else min_down:.2f} Mbps")
        print(f"Ping 最好: {0 if best_ping == float('inf') else best_ping:.1f} ms")
        print(f"Ping 最差: {0 if worst_ping == float('-inf') else worst_ping:.1f} ms")
        if last_avg is not None:
            print(f"最后一次平均延迟: {last_avg:.1f} ms （目标 {last_host}）")
        print(f"抖动最小/最大: {0 if min_jitter == float('inf') else min_jitter:.1f} / "
              f"{0 if max_jitter == float('-inf') else max_jitter:.1f} ms")
        print(f"丢包率最小/最大: {0 if min_loss == float('inf') else min_loss:.1f} / "
              f"{0 if max_loss == float('-inf') else max_loss:.1f} %")
        print("================================")

# ========== 入口 ==========
if __name__ == "__main__":
    # 每 2 秒刷新；每 10 秒做一次多包 ping；每次 ping 发 5 个包
    monitor(interval=DEFAULT_INTERVAL,
            burst_bytes=DEFAULT_BURST,
            ping_every_sec=DEFAULT_PING_EVERY_SEC,
            ping_count=DEFAULT_PING_COUNT)
