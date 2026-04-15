# -*- coding: utf-8 -*-
"""
股票涨跌停状态监控（双速轮询版）

核心逻辑：
1. 慢速轮询：每 10 秒按 400 只一组请求全市场快照，做全量状态判断。
2. 高速轮询：维护最多 400 只“高优先级股票”，每 3 秒轮询一次，提升涨跌停捕捉实时性。
3. 状态机：维护四个状态集合（涨停、跌停、涨停炸板、跌停炸板），仅在状态变化时推送事件。
4. 推送方式：事件发布到 Redis 新频道，方便独立消费端订阅。
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set

import redis

from utils import make_request

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


SLOW_INTERVAL_SECONDS = 10
FAST_INTERVAL_SECONDS = 3
BATCH_SIZE = 400
FAST_POOL_MAX_SIZE = 400

EVENT_CHANNEL = "stock_limit_status_v2:events"
KEY_LIMIT_UP = "stock_limit_status_v2:limit_up"
KEY_LIMIT_DOWN = "stock_limit_status_v2:limit_down"
KEY_LIMIT_UP_BROKEN = "stock_limit_status_v2:limit_up_broken"
KEY_LIMIT_DOWN_BROKEN = "stock_limit_status_v2:limit_down_broken"


def chunks(data: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(data), size):
        yield data[i : i + size]


def parse_change_rate(row: Dict) -> float:
    pct_chg = row.get("pct_chg")
    if pct_chg not in (None, ""):
        try:
            return float(pct_chg)
        except Exception:
            pass

    close = row.get("close")
    pre_close = row.get("pre_close")
    try:
        close_v = float(close or 0)
        pre_close_v = float(pre_close or 0)
        if pre_close_v <= 0:
            return 0.0
        return (close_v - pre_close_v) / pre_close_v * 100
    except Exception:
        return 0.0


def normalize_code(code: str) -> str:
    if not code:
        return ""
    code = str(code).strip().upper()
    if "." in code:
        return code
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    if code.startswith(("8", "4")):
        return f"{code}.BJ"
    return code


def get_board_limit_pct(stock_code: str, stock_name: str) -> float:
    code = stock_code.upper()
    name = (stock_name or "").upper()

    # ST 默认 5%
    if "ST" in name:
        return 5.0

    # 科创/创业 20%
    if code.startswith(("688", "689")) and code.endswith(".SH"):
        return 20.0
    if code.startswith(("300", "301")) and code.endswith(".SZ"):
        return 20.0

    # 北交所 30%
    if code.endswith(".BJ") or code.startswith(("8", "4")):
        return 30.0

    # 主板默认 10%
    return 10.0


def is_trading_window(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False

    hhmmss = now.strftime("%H:%M:%S")
    in_morning = "09:25:00" <= hhmmss <= "11:30:00"
    in_afternoon = "13:00:00" <= hhmmss <= "15:00:00"
    return in_morning or in_afternoon


def is_before_limit_check_time(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now()
    return now.strftime("%H:%M:%S") < "09:25:00"


def is_after_auction_end(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now()
    return now.strftime("%H:%M:%S") >= "09:25:00"


class LimitStatusMonitor:
    def __init__(self):
        redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        redis_db = int(os.getenv("REDIS_DB", "0"))
        redis_password = os.getenv("REDIS_PASSWORD", None)

        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10,
        )

        self.stock_codes: List[str] = []
        self.stock_name_map: Dict[str, str] = {}

        self.fast_pool: Set[str] = set()

        self.limit_up: Set[str] = set()
        self.limit_down: Set[str] = set()
        self.limit_up_broken: Set[str] = set()
        self.limit_down_broken: Set[str] = set()

        self.last_reload_ts = 0.0
        self.last_slow_run_ts = 0.0
        self.last_fast_run_ts = 0.0
        self.current_trade_date = ""

    def reset_daily_state(self, trade_date: str):
        # 清空内存状态，避免上一交易日污染。
        self.fast_pool.clear()
        self.limit_up.clear()
        self.limit_down.clear()
        self.limit_up_broken.clear()
        self.limit_down_broken.clear()

        # 清空 Redis 对应集合，避免消费端读取到昨日状态。
        try:
            self.redis_client.delete(
                KEY_LIMIT_UP,
                KEY_LIMIT_DOWN,
                KEY_LIMIT_UP_BROKEN,
                KEY_LIMIT_DOWN_BROKEN,
            )
        except Exception as e:
            logging.error("重置 Redis 日切状态失败: %s", e)

        self.current_trade_date = trade_date
        logging.info("已完成日切重置，当前交易日: %s", trade_date)

    def ensure_daily_reset(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.current_trade_date != today:
            self.reset_daily_state(today)

    def load_stock_list(self, force: bool = False):
        now_ts = time.time()
        if not force and now_ts - self.last_reload_ts < 300:
            return

        data = make_request("/api/stock/list", method="GET")
        if not data or "list" not in data:
            logging.warning("获取股票列表失败，保留旧列表继续运行")
            return

        codes: List[str] = []
        name_map: Dict[str, str] = {}
        for item in data["list"]:
            code = normalize_code(item.get("stock_code", ""))
            if not code:
                continue

            # 仅监控 A 股（SH/SZ/BJ）
            if not (code.endswith(".SH") or code.endswith(".SZ") or code.endswith(".BJ")):
                continue

            name = item.get("stock_name") or item.get("name") or ""
            name_map[code] = str(name)
            codes.append(code)

        self.stock_codes = sorted(set(codes))
        self.stock_name_map = name_map
        self.last_reload_ts = now_ts
        logging.info("股票列表已刷新，共 %s 只", len(self.stock_codes))

    def fetch_snapshot_batch(self, code_batch: List[str]) -> List[Dict]:
        if not code_batch:
            return []

        today = datetime.now().strftime("%Y-%m-%d")
        payload = {
            "stock_code": code_batch,
            "date": today,
            "page": 0,
            "page_size": 10000,
        }
        data = make_request("/api/stock/snapshot_daily", payload, method="POST")
        if not data or "list" not in data:
            return []
        return data["list"] or []

    def is_high_priority_candidate(self, stock_code: str, change_rate: float) -> bool:
        # 高速池阈值：主板 8.1%，科创/创业 16%
        # 对跌停方向同样使用绝对值阈值，提升跌停识别实时性。
        if stock_code.startswith(("300", "301")) and stock_code.endswith(".SZ"):
            return abs(change_rate) >= 16.0
        if stock_code.startswith(("688", "689")) and stock_code.endswith(".SH"):
            return abs(change_rate) >= 16.0
        return abs(change_rate) >= 8.1

    def get_limit_flags(self, row: Dict) -> Dict[str, bool]:
        code = normalize_code(row.get("stock_code", ""))
        name = row.get("stock_name") or self.stock_name_map.get(code, "")
        change_rate = parse_change_rate(row)
        limit_pct = get_board_limit_pct(code, str(name))

        # 使用保护边界减少四舍五入误判
        up_hit = change_rate >= (limit_pct - 0.5)
        down_hit = change_rate <= -(limit_pct - 0.5)
        return {
            "is_limit_up": up_hit,
            "is_limit_down": down_hit,
            "change_rate": change_rate,
            "limit_pct": limit_pct,
            "stock_code": code,
            "stock_name": str(name),
        }

    def publish_event(self, event: Dict):
        event["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = json.dumps(event, ensure_ascii=False)
        self.redis_client.publish(EVENT_CHANNEL, payload)
        logging.info("事件推送: %s %s", event.get("type"), event.get("stock_code"))

    def sync_redis_sets(self, event_type: str, stock_code: str):
        if event_type == "limit_up":
            self.redis_client.sadd(KEY_LIMIT_UP, stock_code)
            self.redis_client.srem(KEY_LIMIT_UP_BROKEN, stock_code)
        elif event_type == "limit_up_reseal":
            self.redis_client.sadd(KEY_LIMIT_UP, stock_code)
            self.redis_client.srem(KEY_LIMIT_UP_BROKEN, stock_code)
        elif event_type == "limit_up_broken":
            self.redis_client.srem(KEY_LIMIT_UP, stock_code)
            self.redis_client.sadd(KEY_LIMIT_UP_BROKEN, stock_code)
        elif event_type == "limit_down":
            self.redis_client.sadd(KEY_LIMIT_DOWN, stock_code)
            self.redis_client.srem(KEY_LIMIT_DOWN_BROKEN, stock_code)
        elif event_type == "limit_down_reseal":
            self.redis_client.sadd(KEY_LIMIT_DOWN, stock_code)
            self.redis_client.srem(KEY_LIMIT_DOWN_BROKEN, stock_code)
        elif event_type == "limit_down_broken":
            self.redis_client.srem(KEY_LIMIT_DOWN, stock_code)
            self.redis_client.sadd(KEY_LIMIT_DOWN_BROKEN, stock_code)

    def process_row(self, row: Dict, source: str):
        if is_before_limit_check_time():
            return

        flags = self.get_limit_flags(row)
        code = flags["stock_code"]
        if not code:
            return

        name = flags["stock_name"]
        change_rate = float(flags["change_rate"])
        is_limit_up = flags["is_limit_up"]
        is_limit_down = flags["is_limit_down"]

        if is_limit_up:
            if code not in self.limit_up:
                if code in self.limit_up_broken:
                    self.limit_up_broken.discard(code)
                    self.limit_up.add(code)
                    event_type = "limit_up_reseal"
                else:
                    self.limit_up.add(code)
                    event_type = "limit_up"

                event = {
                    "type": event_type,
                    "stock_code": code,
                    "stock_name": name,
                    "change_rate": round(change_rate, 3),
                    "source": source,
                }
                self.publish_event(event)
                self.sync_redis_sets(event_type, code)
        else:
            if code in self.limit_up:
                self.limit_up.discard(code)
                self.limit_up_broken.add(code)
                event = {
                    "type": "limit_up_broken",
                    "stock_code": code,
                    "stock_name": name,
                    "change_rate": round(change_rate, 3),
                    "source": source,
                }
                self.publish_event(event)
                self.sync_redis_sets("limit_up_broken", code)

        if is_limit_down:
            if code not in self.limit_down:
                if code in self.limit_down_broken:
                    self.limit_down_broken.discard(code)
                    self.limit_down.add(code)
                    event_type = "limit_down_reseal"
                else:
                    self.limit_down.add(code)
                    event_type = "limit_down"

                event = {
                    "type": event_type,
                    "stock_code": code,
                    "stock_name": name,
                    "change_rate": round(change_rate, 3),
                    "source": source,
                }
                self.publish_event(event)
                self.sync_redis_sets(event_type, code)
        else:
            if code in self.limit_down:
                self.limit_down.discard(code)
                self.limit_down_broken.add(code)
                event = {
                    "type": "limit_down_broken",
                    "stock_code": code,
                    "stock_name": name,
                    "change_rate": round(change_rate, 3),
                    "source": source,
                }
                self.publish_event(event)
                self.sync_redis_sets("limit_down_broken", code)

        # 竞价结束后才开始将股票分流到高速池
        if is_after_auction_end() and self.is_high_priority_candidate(code, change_rate):
            if len(self.fast_pool) < FAST_POOL_MAX_SIZE:
                self.fast_pool.add(code)

    def slow_scan_once(self):
        if not self.stock_codes:
            return

        total_rows = 0
        for code_batch in chunks(self.stock_codes, BATCH_SIZE):
            rows = self.fetch_snapshot_batch(code_batch)
            total_rows += len(rows)
            for row in rows:
                self.process_row(row, source="slow")

        logging.info(
            "慢速轮询完成: stocks=%s, rows=%s, fast_pool=%s, up=%s, down=%s",
            len(self.stock_codes),
            total_rows,
            len(self.fast_pool),
            len(self.limit_up),
            len(self.limit_down),
        )

    def fast_scan_once(self):
        if not self.fast_pool:
            return

        current_pool = list(self.fast_pool)[:FAST_POOL_MAX_SIZE]
        keep_codes: Set[str] = set()
        total_rows = 0

        for code_batch in chunks(current_pool, BATCH_SIZE):
            rows = self.fetch_snapshot_batch(code_batch)
            total_rows += len(rows)
            for row in rows:
                self.process_row(row, source="fast")
                code = normalize_code(row.get("stock_code", ""))
                if not code:
                    continue
                change_rate = parse_change_rate(row)
                # 仍处于高波动区，或者仍在封板状态时，继续留在高速池
                if (
                    self.is_high_priority_candidate(code, change_rate)
                    or code in self.limit_up
                    or code in self.limit_down
                ):
                    keep_codes.add(code)

        self.fast_pool = keep_codes
        logging.info("高速轮询完成: rows=%s, fast_pool=%s", total_rows, len(self.fast_pool))

    def run(self):
        logging.info("启动涨跌停双速监控服务")
        self.ensure_daily_reset()
        self.load_stock_list(force=True)

        while True:
            self.ensure_daily_reset()
            now = time.time()
            if not is_trading_window():
                # 非交易时段每分钟刷新一次列表，避免长时间运行后列表老化
                if now - self.last_reload_ts > 60:
                    self.load_stock_list(force=True)
                time.sleep(1)
                continue

            if now - self.last_reload_ts > 300:
                self.load_stock_list(force=True)

            if now - self.last_slow_run_ts >= SLOW_INTERVAL_SECONDS:
                self.slow_scan_once()
                self.last_slow_run_ts = time.time()

            # 竞价结束前不分高速组和普通组，只跑统一慢速逻辑
            if not is_after_auction_end():
                if self.fast_pool:
                    self.fast_pool.clear()
                time.sleep(0.2)
                continue

            if now - self.last_fast_run_ts >= FAST_INTERVAL_SECONDS:
                self.fast_scan_once()
                self.last_fast_run_ts = time.time()

            time.sleep(0.2)


def main():
    if os.getenv("ALLOW_STANDALONE_LIMIT_STATUS", "0") != "1":
        logging.warning("已禁用独立运行 fetch_limit_status.py，避免与 fetch_realtime 重复轮询。")
        logging.warning("如需强制独立运行，请设置环境变量 ALLOW_STANDALONE_LIMIT_STATUS=1。")
        return

    monitor = LimitStatusMonitor()
    monitor.run()


if __name__ == "__main__":
    main()
