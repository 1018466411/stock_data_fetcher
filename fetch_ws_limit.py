# -*- coding: utf-8 -*-
"""
使用说明：
本脚本用于连接 c_api 新增的 WS 路由 `/ws/stream`，接收涨跌停相关事件。

默认行为：
1. 订阅 `stock_limit_all`（涨停/跌停/炸板/回封全量事件）。
2. 自动心跳（每 20 秒发送 ping）。
3. 自动重连（断开后 5 秒重连）。
4. 可选写入 ClickHouse 表 `ws_data`（默认开启）。
"""

import json
import logging
import os
import threading
import time
from datetime import datetime

import websocket

from db import get_config, get_db, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

config = get_config()
write_db = os.getenv("LIMIT_WS_WRITE_DB", "1") == "1"

ws_app = None


def get_ws_url() -> str:
    api_key = "c051b919e52ef9b2b9eda39247b9c16a5afe24bd17db8b3e58"
    if not api_key:
        logging.error("未配置 API Key，程序退出。")
        os._exit(1)

    domain = config["api"]["domain"].rstrip("/")
    ws_base = domain.replace("https://", "wss://").replace("http://", "ws://")
    # 只订阅涨跌停相关事件
    return f"{ws_base}/ws/stream?token={api_key}&types=stock_limit_all"


def save_to_db(event: dict):
    if not write_db:
        return
    try:
        db = get_db()
        stock_code = ""
        payload = event
        if event.get("type") == "stock_limit_event" and isinstance(event.get("data"), dict):
            payload = event["data"]
            stock_code = payload.get("stock_code", "")
        else:
            stock_code = event.get("stock_code", "")

        db.insert(
            "ws_data",
            [
                {
                    "stock_code": stock_code or "UNKNOWN",
                    "push_time": datetime.now(),
                    "data_type": "stock_limit_event",
                    "content": json.dumps(payload, ensure_ascii=False),
                }
            ],
        )
    except Exception as e:
        logging.error(f"写入数据库失败: {e}")


def handle_message(obj: dict):
    msg_type = obj.get("type", "")

    if msg_type == "stock_limit_event":
        data = obj.get("data", {})
        event_type = data.get("type", "")
        stock_code = data.get("stock_code", "")
        stock_name = data.get("stock_name", "")
        change_rate = data.get("change_rate", "")
        source = data.get("source", "")
        ts = data.get("timestamp", "")
        logging.warning(
            "涨跌停事件: %s %s(%s) 涨跌幅=%s 来源=%s 时间=%s",
            event_type,
            stock_name,
            stock_code,
            change_rate,
            source,
            ts,
        )
        threading.Thread(target=save_to_db, args=(obj,), daemon=True).start()
        return

    if msg_type == "force_disconnect":
        reason = obj.get("reason", "server force disconnect")
        logging.warning(f"收到强制断开指令: {reason}")
        try:
            ws_app.close()
        except Exception:
            pass
        return

    if msg_type == "pong":
        return

    # 首次连接响应、订阅响应等
    if obj.get("code") == 200:
        logging.info(f"WS响应: {obj.get('msg', '')} data={obj.get('data', {})}")
    else:
        logging.info(f"收到消息: {obj}")


def on_message(ws, message):
    try:
        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="ignore")
        obj = json.loads(message)
        handle_message(obj)
    except Exception as e:
        logging.error(f"处理消息失败: {e}, raw={message}")


def on_error(ws, error):
    logging.error(f"WebSocket 发生错误: {error}")
    error_str = str(error)
    if any(x in error_str for x in ["401", "402", "403", "Handshake status 401", "Handshake status 402", "Handshake status 403"]):
        logging.error("WebSocket 鉴权失败或无权限，程序退出。")
        os._exit(1)


def on_close(ws, close_status_code, close_msg):
    logging.warning(f"WebSocket 连接已关闭 (Status: {close_status_code}, Msg: {close_msg})，准备重连...")
    if close_status_code in [4001, 4003, 401, 402, 403]:
        logging.error("WebSocket 因权限问题被关闭，程序退出。")
        os._exit(1)


def heartbeat_loop():
    while True:
        time.sleep(20)
        try:
            if ws_app and ws_app.sock and ws_app.sock.connected:
                ws_app.send(json.dumps({"action": "ping"}))
        except Exception:
            pass


def on_open(ws):
    logging.info("WebSocket 连接已建立。")
    # 显式再发一次订阅，便于后续动态扩展和日志确认。
    sub_msg = {"action": "subscribe", "types": ["stock_limit_all"]}
    ws.send(json.dumps(sub_msg, ensure_ascii=False))


def main():
    global write_db
    if write_db:
        try:
            init_db()
        except Exception as e:
            logging.warning(f"ClickHouse 初始化失败，自动切换为仅接收模式（不落库）: {e}")
            write_db = False

    threading.Thread(target=heartbeat_loop, daemon=True).start()

    while True:
        ws_url = get_ws_url()
        logging.info(f"准备连接 WebSocket: {ws_url}")

        global ws_app
        ws_app = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws_app.run_forever(ping_interval=30, ping_timeout=10)
        time.sleep(5)


if __name__ == "__main__":
    main()
