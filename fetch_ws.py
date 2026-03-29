# -*- coding: utf-8 -*-
"""
使用说明：
本脚本用于连接并接收 WebSocket 推送的实时股票数据，并将接收到的数据直接存入 ClickHouse 数据库中。
数据采用 ReplacingMergeTree 引擎，以确保在相同时刻同一类型的数据不会出现重复。

启动方式：
直接运行本脚本即可，脚本具有自动重连机制。
如有特定的订阅频道或认证逻辑，请在 on_open 中修改对应发送的订阅消息。
"""

import websocket
import json
import logging
import time
import threading
from datetime import datetime
from db import get_ch_client, get_config, init_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 全局 ClickHouse 客户端和配置
ch_client = None
config = get_config()

def get_ws_url():
    api_key = config['api'].get('api_key') or __import__('db').get_api_key()
    if not api_key:
        logging.error("未配置 API Key，程序退出。")
        import os
        os._exit(1)
    return config['api']['domain'].replace("https://", "wss://").replace("http://", "ws://") + f"/ws/stock/snapshot?token={api_key}"

def save_to_db(data_list):
    """批量写入 ClickHouse"""
    if not data_list:
        return
        
    try:
        # 为每个线程创建一个新的客户端，避免并发查询报错
        client = get_ch_client()
        client.execute(
            'INSERT INTO stock_ws_data (stock_code, push_time, data_type, content) VALUES',
            data_list
        )
    except Exception as e:
        logging.error(f"写入数据库失败: {e}")

def on_message(ws, message):
    """处理接收到的 WebSocket 消息"""
    try:
        # 如果是 bytes，可能需要解压或解码，这里根据常见的 gzip 或者 utf-8 处理
        if isinstance(message, bytes):
            import zlib
            try:
                # 尝试解压 gzip 数据 (带有 header 和 trailer)
                message = zlib.decompress(message, 16 + zlib.MAX_WBITS).decode('utf-8')
            except zlib.error:
                try:
                    # 尝试解压 deflate 数据 (无 header 和 trailer)
                    message = zlib.decompress(message, -zlib.MAX_WBITS).decode('utf-8')
                except:
                    message = message.decode('utf-8', errors='ignore')
            except:
                message = message.decode('utf-8', errors='ignore')
                
        data = json.loads(message)
        # 假设推送的数据格式为:
        # {"action": "push", "data_type": "trade", "stock_code": "600000.SH", "time": "2023-01-01 10:00:00", "content": {...}}
        # 根据实际业务进行调整
        
        # 针对从 gzip/deflate 解压出来的列表进行处理
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # 可能是 {"data": [...]} 或者单条数据
            records = data.get('data', [data])
        else:
            return
            
        insert_data = []
        for item in records:
            stock_code = item.get('cd', item.get('stock_code', 'UNKNOWN'))
            push_time = item.get('ut', item.get('time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            if isinstance(push_time, str):
                if len(push_time) == 10:
                    push_time = datetime.strptime(push_time, '%Y-%m-%d')
                else:
                    push_time = datetime.strptime(push_time, '%Y-%m-%d %H:%M:%S')
            data_type = item.get('data_type', 'snapshot')
            # 将具体内容转为字符串保存
            content = json.dumps(item.get('content', item), ensure_ascii=False)
            
            insert_data.append((stock_code, push_time, data_type, content))
            
        if insert_data:
            # 使用新线程异步写入数据库，避免阻塞 WS 接收
            threading.Thread(target=save_to_db, args=(insert_data,)).start()
            
    except json.JSONDecodeError:
        logging.warning(f"无法解析的消息: {message}")
    except Exception as e:
        logging.error(f"处理消息时发生错误: {e}")

def on_error(ws, error):
    logging.error(f"WebSocket 发生错误: {error}")
    error_str = str(error)
    if '401' in error_str or '403' in error_str or 'Handshake status 401' in error_str or 'Handshake status 403' or 'Handshake status 402' in error_str:
        logging.error("WebSocket 鉴权失败或无权限，程序退出。")
        import os
        os._exit(1)

def on_close(ws, close_status_code, close_msg):
    logging.warning(f"WebSocket 连接已关闭 (Status: {close_status_code}, Msg: {close_msg})，准备重连...")
    if close_status_code in [4001, 4003, 403, 401, 402]:
        logging.error("WebSocket 因权限问题被关闭，程序退出。")
        import os
        os._exit(1)

def on_open(ws):
    logging.info("WebSocket 连接已建立。")
    # 如果需要发送订阅消息，可以在此处发送
    # sub_msg = {"action": "subscribe", "channels": ["realtime", "snapshot"]}
    # ws.send(json.dumps(sub_msg))

def main():
    global ch_client
    init_db()
    ch_client = get_ch_client()
    
    # 启用自动重连的 WebSocket 应用
    while True:
        ws_url = get_ws_url()
        logging.info(f"准备连接 WebSocket: {ws_url}")
        
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        # 运行 WebSocket，ping_interval 保持连接心跳
        ws.run_forever(ping_interval=30, ping_timeout=10)
        
        # 断开后等待 5 秒重连
        time.sleep(5)

if __name__ == "__main__":
    main()