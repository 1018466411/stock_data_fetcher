# -*- coding: utf-8 -*-
"""
使用说明：
本脚本用于独立获取当天的实时快照数据 (Snapshot)。
执行逻辑：
1. 每 3 秒执行一次。
2. 检查是否在交易时间内。
3. 调用全市场快照接口，获取数据并存入 stock_realtime_snapshot。
"""

import sys
import time
import schedule
import logging
import threading
from datetime import datetime
from db import get_ch_client, get_config, init_db
from utils import make_request

# Ensure unbuffered output so logs appear immediately in subprocess pipes
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

def is_trading_time():
    """判断当前时间是否为交易时间 (简单判断，不考虑节假日)"""
    return True # FOR TESTING ONLY

def fetch_and_save_snapshot():
    """获取并保存实时快照数据"""
    page = 1
    page_size = 500
    client = get_ch_client()
    
    total_inserted = 0
    while True:
        try:
            data = make_request("/api/stock/snapshot_daily", {"page": page, "page_size": page_size}, method='POST')
            if not data or 'list' not in data:
                break
                
            records = data['list']
            if not records:
                break
                
            insert_data = []
            for row in records:
                # 转换为合适的数组，如果接口返回的不是数组需要处理
                buy_vols = [float(x) for x in row.get('buy_vol', [])] if isinstance(row.get('buy_vol'), list) else []
                buy_prices = [float(x) for x in row.get('buy_price', [])] if isinstance(row.get('buy_price'), list) else []
                sell_vols = [float(x) for x in row.get('sell_vol', [])] if isinstance(row.get('sell_vol'), list) else []
                sell_prices = [float(x) for x in row.get('sell_price', [])] if isinstance(row.get('sell_price'), list) else []
                
                t_time = row.get('trade_time') or row.get('snapshot_time') or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if isinstance(t_time, str):
                    if len(t_time) == 10:
                        t_time = datetime.strptime(t_time, '%Y-%m-%d')
                    else:
                        t_time = datetime.strptime(t_time, '%Y-%m-%d %H:%M:%S')

                insert_data.append((
                    row.get('stock_code'),
                    t_time,
                    float(row.get('current_price', 0) or row.get('price', 0)),
                    float(row.get('open', 0)),
                    float(row.get('high', 0)),
                    float(row.get('low', 0)),
                    float(row.get('volume', 0)),
                    float(row.get('amount', 0)),
                    buy_vols,
                    buy_prices,
                    sell_vols,
                    sell_prices
                ))
                
            if insert_data:
                client.execute(
                    '''INSERT INTO stock_realtime_snapshot 
                       (stock_code, snapshot_time, price, open, high, low, volume, amount, 
                       buy_vols, buy_prices, sell_vols, sell_prices) VALUES''',
                    insert_data
                )
                total_inserted += len(insert_data)
                
            if len(records) < page_size:
                break
            page += 1
        except Exception as e:
            logging.error(f"获取快照数据发生错误: {e}")
            break
            
    if total_inserted > 0:
        logging.info(f"成功获取并写入 {total_inserted} 条快照数据")

def snapshot_sync_job():
    if not is_trading_time():
        return
    logging.info("开始获取实时快照数据...")
    fetch_and_save_snapshot()

def run_job_in_thread():
    threading.Thread(target=snapshot_sync_job).start()

def main():
    init_db()
    logging.info("实时快照服务已启动，每 3 秒执行一次...")
    
    schedule.every(3).seconds.do(run_job_in_thread)
    
    while True:
        schedule.run_pending()
        time.sleep(0.5)

if __name__ == '__main__':
    main()