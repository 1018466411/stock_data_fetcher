# -*- coding: utf-8 -*-
"""
使用说明：
本脚本用于获取股票的历史数据（分钟级、日级）以及财务数据，并写入 ClickHouse 数据库。
支持的级别包括：1m, 5m, 15m, 30m, 60m, daily
可以同时下载多个级别。数据不存在重复，如有新数据会自动覆盖。
脚本内置限速处理（触发限速自动等待5秒），并开启了多线程执行加速获取。
每次请求最多 10000 条数据，如果超过会自动分页获取。
默认执行时间范围：分钟级从 2000-06-01 开始，日级从 1990-01-01 开始，至当前时间。

参数说明 (可通过命令行传递或修改代码里的默认参数):
--levels: 指定要获取的数据级别，多个用逗号分隔，如: 1m,5m,daily。默认获取所有。 
--start_time: 指定开始时间，格式如 '2023-01-01 00:00:00'，不传则获取全部。      
--end_time: 指定结束时间，格式如 '2023-01-10 00:00:00'，不传则默认为脚本运行的当前时间。
--rate_limit: 每分钟最大请求次数，默认 300。用于防止请求过快被服务器限制。

下面是一个例子（在docker里面安装）：
docker exec stock_fetcher_app python fetch_history.py --levels 1m,daily --start_time '2023-01-01 00:00:00' --end_time '2026-04-10 00:00:00' --rate_limit 300
"""

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime
from db import get_ch_client, get_config, init_db
from utils import make_request
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_stock_list():
    """
    获取股票列表及其上市时间
    """
    logging.info("正在获取股票列表...")
    # 这里假设有获取股票列表的接口 /api/stock/list，包含 stock_code 和 list_date (上市时间)
    # 具体接口路径需要根据实际情况调整
    data = make_request("/api/stock/list", method='GET')
    if not data or 'list' not in data:
        logging.warning("未能获取到股票列表或接口未返回 list 字段，返回空列表。请检查接口是否正确。")
        return []
    return data['list']

def fetch_and_save_finance(stock_code, start_time, end_time):
    """获取并保存财务数据"""
    from db import get_db
    db = get_db()
    
    # 截取日期部分 YYYY-MM-DD
    start_date = start_time.split(' ')[0] if start_time else None
    end_date = end_time.split(' ')[0] if end_time else None
    
    params = {
        'stock_code': stock_code,
        'start_time': start_date,
        'end_time': end_date
    }
    # 不传递 page 和 page_size 参数，因为该接口的分页参数会导致服务端返回数据错乱甚至为空
    data = make_request("/api/stock/finance", params, method='POST')
    if not data or 'list' not in data:
        return
        
    records = data['list']
    if not records:
        return
        
    # 写入 clickhouse
    insert_data = []
    for row in records:
        # 字段需要和数据库对应
        trade_date = row.get('trade_date')
        if trade_date and isinstance(trade_date, str):
            trade_date = datetime.strptime(trade_date.split(' ')[0], '%Y-%m-%d').date()
            
        insert_data.append({
            'stock_code': stock_code,
            'trade_date': trade_date,
            'close': float(row.get('close', 0) or 0),
            'turnover_rate': float(row.get('turnover_rate', 0) or 0),
            'turnover_rate_f': float(row.get('turnover_rate_f', 0) or 0),
            'volume_ratio': float(row.get('volume_ratio', 0) or 0),
            'pe': float(row.get('pe', 0) or 0),
            'pe_ttm': float(row.get('pe_ttm', 0) or 0),
            'pe_ttm_percentile': float(row.get('pe_ttm_percentile', 0) or 0),
            'pb': float(row.get('pb', 0) or 0),
            'ps': float(row.get('ps', 0) or 0),
            'ps_ttm': float(row.get('ps_ttm', 0) or 0),
            'dv_ratio': float(row.get('dv_ratio', 0) or 0),
            'dv_ttm': float(row.get('dv_ttm', 0) or 0),
            'total_share': float(row.get('total_share', 0) or 0),
            'float_share': float(row.get('float_share', 0) or 0),
            'free_share': float(row.get('free_share', 0) or 0),
            'total_mv': float(row.get('total_mv', 0) or 0),
            'circ_mv': float(row.get('circ_mv', 0) or 0)
        })
    
    if insert_data:
        db.insert('finance', insert_data)

def fetch_and_save_history(stock_code, level, start_time, end_time):
    """获取并保存历史K线数据"""
    page = 0
    page_size = 10000
    from db import get_db
    db = get_db()
    
    while True:
        params = {
            'stock_code': stock_code,
            'start_time': start_time,
            'end_time': end_time,
            'page': page,
            'page_size': page_size
        }
        if level == 'daily':
            endpoint = "/api/stock/daily"
            params['start_time'] = start_time.split(' ')[0] if start_time else None
            params['end_time'] = end_time.split(' ')[0] if end_time else None
        else:
            endpoint = "/api/stock/history"
            # history 接口的时间必须精确到秒
            params['start_time'] = start_time if len(start_time) > 10 else f"{start_time} 00:00:00"
            params['end_time'] = end_time if len(end_time) > 10 else f"{end_time} 00:00:00"
            level_map = {'1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min', '60m': '60min'}
            params['level'] = level_map.get(level, level)
            
        data = make_request(endpoint, params, method='POST')
        if not data or 'list' not in data:
            break
            
        records = data['list']
        if not records:
            break
            
        insert_data = []
        for row in records:
            trade_time = row.get('trade_time') or row.get('trade_date')
            if trade_time and isinstance(trade_time, str):
                if len(trade_time) == 10:
                    trade_time = datetime.strptime(trade_time, '%Y-%m-%d')      
                else:
                    trade_time = datetime.strptime(trade_time, '%Y-%m-%d %H:%M:%S')
            volume = row.get('volume') or row.get('vol', 0)
            insert_data.append({
                'stock_code': stock_code,
                'trade_time': trade_time,
                'open': float(row.get('open', 0)),
                'close': float(row.get('close', 0)),
                'high': float(row.get('high', 0)),
                'low': float(row.get('low', 0)),
                'volume': float(volume),
                'amount': float(row.get('amount', 0))
            })

        if insert_data:
            table_key = f"history_{level}"
            db.insert(table_key, insert_data)
            
        if len(records) < page_size:
            break
        page += 1

def fill_gaps(stock_code, levels, start_time, end_time):
    logging.info(f"开始检查并补缺 {stock_code} 的数据...")
    page = 0
    page_size = 10000
    baseline_dates = set()
    
    start_date = start_time.split(' ')[0] if start_time else None
    end_date = end_time.split(' ')[0] if end_time else None
    
    while True:
        params = {
            'stock_code': stock_code,
            'start_time': start_date,
            'end_time': end_date,
            'page': page,
            'page_size': page_size
        }
        data = make_request("/api/stock/daily", params, method='POST')
        if not data or 'list' not in data:
            break
        records = data['list']
        if not records:
            break
        for row in records:
            trade_time = row.get('trade_time') or row.get('trade_date')
            if trade_time:
                if isinstance(trade_time, str):
                    date_str = trade_time.split(' ')[0]
                    baseline_dates.add(date_str)
        if len(records) < page_size:
            break
        page += 1

    if not baseline_dates:
        logging.info(f"{stock_code} 没有基准日K数据，跳过补缺。")
        return

    from db import get_db
    client = get_db()
    
    for level in levels:
        table_config = get_config().get('tables', {}).get(f'history_{level}', {})
        table_name = table_config.get('name', f"stock_history_{level}")
        fields = table_config.get('fields', {})
        trade_time_col = fields.get('trade_time', 'trade_time')
        stock_code_col = fields.get('stock_code', 'stock_code')
        
        query = f"SELECT DISTINCT toDate({trade_time_col}) FROM {table_name} WHERE {stock_code_col} = '{stock_code}'"
        if start_date:
            query += f" AND {trade_time_col} >= '{start_date} 00:00:00'"
        if end_date:
            query += f" AND {trade_time_col} <= '{end_date} 23:59:59'"
            
        try:
            existing_dates_tuples = client.execute(query)
            existing_dates = set(str(d[0]) for d in existing_dates_tuples)
        except Exception as e:
            logging.error(f"查询数据库失败: {e}")
            existing_dates = set()
            
        missing_dates = sorted(list(baseline_dates - existing_dates))
        if missing_dates:
            logging.info(f"{stock_code} {level} 级别缺少 {len(missing_dates)} 天的数据，开始补缺...")
            dates_obj = [datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates]
            intervals = []
            start = dates_obj[0]
            end = dates_obj[0]
            for d in dates_obj[1:]:
                if (d - end).days == 1:
                    end = d
                else:
                    intervals.append((start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
                    start = d
                    end = d
            intervals.append((start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
            
            for s_date, e_date in intervals:
                s_time = f"{s_date} 00:00:00"
                e_time = f"{e_date} 23:59:59"
                logging.info(f"补缺 {stock_code} {level} 数据: {s_time} 至 {e_time}")
                fetch_and_save_history(stock_code, level, s_time, e_time)
        else:
            logging.info(f"{stock_code} {level} 级别数据完整，无需补缺。")

def process_stock(stock, levels, custom_start_time, custom_end_time, rate_limit):
    import utils
    utils.RATE_LIMIT = rate_limit
    
    stock_code = stock.get('stock_code')
    list_date_str = stock.get('list_date') # 假设为 'YYYY-MM-DD'
    
    history_levels = [l for l in levels if l != 'finance']
    
    for level in history_levels:
        try:
            # 确定开始时间
            if custom_start_time:
                start_time = custom_start_time
            else:
                default_start = '1990-01-01 00:00:00' if level == 'daily' else '2000-06-01 00:00:00'
                if list_date_str:
                    list_time = list_date_str + ' 00:00:00'
                    start_time = max(default_start, list_time)
                else:
                    start_time = default_start
            
            end_time = custom_end_time or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 如果是日线级别，日志和参数都只保留日期部分
            log_start = start_time.split(' ')[0] if level == 'daily' else start_time
            log_end = end_time.split(' ')[0] if level == 'daily' else end_time
            
            logging.info(f"开始获取 {stock_code} 的 {level} 级别数据，时间段: {log_start} 至 {log_end}")
            fetch_and_save_history(stock_code, level, start_time, end_time)
        except Exception as e:
            import traceback
            logging.error(f"处理股票 {stock_code} 的 {level} 数据时出错: {e}")
            logging.error(traceback.format_exc())
            
    # 获取财务数据 (如果用户勾选了 finance)
    if 'finance' in levels:
        try:
            start_time = custom_start_time or ('1990-01-01' if not list_date_str else list_date_str)
            end_time = custom_end_time or datetime.now().strftime('%Y-%m-%d')
            
            # 只保留日期部分
            start_date = start_time.split(' ')[0] if start_time else None
            end_date = end_time.split(' ')[0] if end_time else None
            
            logging.info(f"开始获取 {stock_code} 的财务数据，时间段: {start_date} 至 {end_date}")
            fetch_and_save_finance(stock_code, start_date, end_date)
        except Exception as e:
            logging.error(f"处理股票 {stock_code} 的财务数据时出错: {e}")
        
    # 数据补缺
    try:
        start_time = custom_start_time or ('1990-01-01 00:00:00' if not list_date_str else list_date_str + ' 00:00:00')
        end_time = custom_end_time or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fill_gaps(stock_code, history_levels, start_time, end_time)
    except Exception as e:
        import traceback
        logging.error(f"处理股票 {stock_code} 数据补缺时出错: {e}")
        logging.error(traceback.format_exc())

def main():
    parser = argparse.ArgumentParser(description="获取股票历史数据脚本")        
    parser.add_argument('--levels', type=str, default='1m,5m,15m,30m,60m,daily', help="要获取的级别，逗号分隔")
    parser.add_argument('--start_time', type=str, default=None, help="开始时间，如 2023-01-01 00:00:00")
    parser.add_argument('--end_time', type=str, default=None, help="结束时间，如 2023-01-10 00:00:00")
    parser.add_argument('--rate_limit', type=int, default=300, help="每分钟请求限速，默认 300 次/分钟")
    args = parser.parse_args()

    levels = [l.strip() for l in args.levels.split(',') if l.strip()]

    # 确保数据库初始化
    init_db()

    stocks = get_stock_list()
    if not stocks:
        logging.error("没有可处理的股票列表，程序退出。")
        return

    config = get_config()
    
    # 采用多进程来提升速度，固定开启8个进程
    max_workers = 8
    
    # 将总限速平均分配到每个进程中，防止并发请求超限
    process_rate_limit = max(1, args.rate_limit // max_workers)
    
    logging.info(f"总限速设置为: {args.rate_limit} 次/分钟，将使用 {max_workers} 个进程处理 {len(stocks)} 只股票。每个进程限速 {process_rate_limit} 次/分钟。")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_stock, stock, levels, args.start_time, args.end_time, process_rate_limit): stock 
            for stock in stocks
        }
        for future in as_completed(futures):
            stock = futures[future]
            try:
                future.result()
                logging.info(f"股票 {stock.get('stock_code')} 所有数据处理完成。")
            except Exception as e:
                logging.error(f"股票 {stock.get('stock_code')} 处理失败: {e}")

if __name__ == '__main__':
    main()