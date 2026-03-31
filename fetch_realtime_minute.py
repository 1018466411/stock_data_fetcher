# -*- coding: utf-8 -*-
"""
使用说明：
本脚本用于独立获取当天的实时分时数据并进行补缺。
执行逻辑：
1. 定时任务：每分钟的第 01 秒触发，获取当前分钟的分时数据。如果失败则每秒重试，最多重试到 60 秒。
2. 更新机制：每分钟的第 15 秒强制再触发一次更新，确保数据最新。
3. 补缺机制：如果当前分钟的数据获取成功，则顺便检查当天 (09:31-11:30, 13:00-15:00) 历史分钟数据的完整性，如有缺失则统一调用历史接口进行补齐。
"""

import sys
import time
import logging
import threading
from datetime import datetime, timedelta
from db import get_ch_client, init_db
from utils import make_request
from fetch_history import get_stock_list
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

def is_trading_time():
    """判断当前时间是否为交易时间"""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
        
    current_time = now.time()
    morning_start = datetime.strptime("09:30", "%H:%M").time()
    morning_end = datetime.strptime("11:30", "%H:%M").time()
    afternoon_start = datetime.strptime("13:00", "%H:%M").time()
    afternoon_end = datetime.strptime("15:00", "%H:%M").time()
    
    if (morning_start <= current_time <= morning_end) or \
       (afternoon_start <= current_time <= afternoon_end):
        return True
    return False

def fetch_and_insert_missing_minute(target_time_str):
    """单独获取并插入某一分钟的数据"""
    try:
        client = get_ch_client()
        logging.info(f"正在补缺时间点: {target_time_str} ...")
        data = make_request("/api/realtime/history", {"trade_time": target_time_str}, method='POST')
        
        records = []
        if isinstance(data, dict) and 'list' in data:
            records = data['list']
        elif isinstance(data, list):
            records = data

        if records:
            insert_data = []
            for row in records:
                t_time = row.get('trade_time')
                if isinstance(t_time, str):
                    if len(t_time) == 10:
                        t_time = datetime.strptime(t_time, '%Y-%m-%d')
                    else:
                        t_time = datetime.strptime(t_time, '%Y-%m-%d %H:%M:%S')
                
                insert_data.append((
                    row.get('stock_code'),
                    t_time,
                    float(row.get('price', 0) or row.get('close', 0)),
                    float(row.get('volume', 0) or row.get('vol', 0)),
                    float(row.get('amount', 0))
                ))
            
            if insert_data:
                client.execute(
                    'INSERT INTO stock_realtime_minute (stock_code, trade_time, price, volume, amount) VALUES',
                    insert_data
                )
                logging.info(f"成功补全时间点 {target_time_str} 的 {len(insert_data)} 条分时数据")
    except Exception as e:
        logging.error(f"补缺时间点 {target_time_str} 失败: {e}")

def check_and_fill_missing_minutes():
    """检查并按时间补缺当天缺失的分时数据"""
    now = datetime.now()
    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
        return
        
    client = get_ch_client()
    today_str = now.strftime('%Y-%m-%d')
    
    # 获取今天预期应该有的所有分钟时间点
    expected_times = []
    current_time = now.time()
    max_time = datetime.strptime("15:00", "%H:%M").time()
    check_end_time = min(current_time, max_time)
    
    # 09:31 到 11:30
    t = datetime.strptime("09:31", "%H:%M")
    while t.time() <= check_end_time and t.time() <= datetime.strptime("11:30", "%H:%M").time():
        expected_times.append(t.strftime("%H:%M"))
        t += timedelta(minutes=1)
        
    # 13:01 到 15:00
    t = datetime.strptime("13:01", "%H:%M")
    while t.time() <= check_end_time and t.time() <= datetime.strptime("15:00", "%H:%M").time():
        expected_times.append(t.strftime("%H:%M"))
        t += timedelta(minutes=1)
        
    # 设定超过4500条数据就认为该分钟数据大致完整
    threshold = 4500
    
    # 查询今天数据库中已经有数据的分钟点
    query = f"""
        SELECT formatDateTime(trade_time, '%H:%M') AS t_time, count(*) as cnt
        FROM stock_realtime_minute
        WHERE toDate(trade_time) = toDate('{today_str}')
        GROUP BY t_time
        HAVING cnt > {threshold}
    """
    try:
        existing_times = [row[0] for row in client.execute(query)]
    except Exception as e:
        logging.error(f"查询已有分时汇总失败: {e}")
        return
        
    missing_times = [t for t in expected_times if t not in existing_times]
    
    if missing_times:
        logging.info(f"全市场缺失 {len(missing_times)} 个分钟点的数据，开始多线程按时间补缺...")
        
        # 控制并发数，避免把接口打挂或触发限速
        max_workers = min(10, len(missing_times))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for missing_time in missing_times:
                target_time_str = f"{today_str} {missing_time}:00"
                futures.append(executor.submit(fetch_and_insert_missing_minute, target_time_str))
            
            # 等待所有补缺任务完成
            for future in as_completed(futures):
                pass
    else:
        logging.info("全市场历史分时数据完整，无需补缺。")

def fetch_current_minute_with_retry(target_time_str, is_update=False):
    """
    尝试获取指定时间的分时数据，如果失败则等待1秒重试。
    如果是正常 01 秒触发，最多重试 55 次。
    如果是 15 秒更新触发，最多重试 5 次。
    获取成功后，触发补缺逻辑。
    """
    client = get_ch_client()
    max_retries = 5 if is_update else 55
    success = False
    
    for attempt in range(max_retries):
        try:
            data = make_request("/api/realtime/history", {"trade_time": target_time_str}, method='POST')
            
            records = []
            if isinstance(data, dict) and 'list' in data:
                records = data['list']
            elif isinstance(data, list):
                records = data
                
            if records:
                insert_data = []
                for row in records:
                    t_time = row.get('trade_time')
                    if isinstance(t_time, str):
                        if len(t_time) == 10:
                            t_time = datetime.strptime(t_time, '%Y-%m-%d')
                        else:
                            t_time = datetime.strptime(t_time, '%Y-%m-%d %H:%M:%S')
                    insert_data.append((
                        row.get('stock_code'),
                        t_time,
                        float(row.get('price', 0) or row.get('close', 0)),
                        float(row.get('volume', 0) or row.get('vol', 0)),
                        float(row.get('amount', 0))
                    ))
                if insert_data:
                    client.execute(
                        'INSERT INTO stock_realtime_minute (stock_code, trade_time, price, volume, amount) VALUES',
                        insert_data
                    )
                action_str = "更新" if is_update else "获取"
                logging.info(f"成功{action_str}并写入 {len(insert_data)} 条分时数据，时间: {target_time_str} (尝试次数: {attempt+1})")
                success = True
                break
            else:
                logging.warning(f"未能获取到分时数据，1秒后重试... (当前尝试: {attempt+1}/{max_retries})")
        except Exception as e:
            logging.error(f"获取分时数据发生错误: {e}")
            
        time.sleep(1)
        
    if success and not is_update:
        # 01 秒触发且成功后，执行全市场补缺
        logging.info("分时获取成功，开始检查历史分时数据完整性...")
        threading.Thread(target=check_and_fill_missing_minutes).start()

def main():
    init_db()
    logging.info("实时分时数据及补缺服务已启动...")
    
    # 启动时的逻辑判断
    now = datetime.now()
    if now.weekday() < 5:
        current_time = now.time()
        start_time = datetime.strptime("09:31", "%H:%M").time()
        end_time = datetime.strptime("15:00", "%H:%M").time()
        
        if current_time > end_time:
            logging.info("启动时间晚于15:00，主动发起一次全市场收盘补缺检查...")
            threading.Thread(target=check_and_fill_missing_minutes).start()
        elif current_time < start_time:
            logging.info("启动时间早于09:31，等待开盘时间到来...")
    
    last_run_01_minute = -1
    last_run_15_minute = -1
    last_not_trading_log_minute = -1
    
    while True:
        if is_trading_time():
            now = datetime.now()
            current_minute = now.minute
            current_second = now.second
            target_time_str = now.strftime('%Y-%m-%d %H:%M:00')
            
            # 在每分钟的第 01 秒触发获取
            if current_second == 1 and last_run_01_minute != current_minute:
                last_run_01_minute = current_minute
                logging.info(f"[{target_time_str}] 触发 01 秒新分时数据获取...")
                threading.Thread(target=fetch_current_minute_with_retry, args=(target_time_str, False)).start()
                
            # 在每分钟的第 15 秒触发强制更新
            if current_second == 15 and last_run_15_minute != current_minute:
                last_run_15_minute = current_minute
                logging.info(f"[{target_time_str}] 触发 15 秒分时数据更新...")
                threading.Thread(target=fetch_current_minute_with_retry, args=(target_time_str, True)).start()
        else:
            now = datetime.now()
            current_minute = now.minute
            if last_not_trading_log_minute != current_minute:
                last_not_trading_log_minute = current_minute
                logging.info(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 当前非交易时间，等待中...")
                
        time.sleep(0.5)

if __name__ == '__main__':
    main()
