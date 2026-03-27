import yaml
from clickhouse_driver import Client
import os

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_ch_client():
    config = get_config()['clickhouse']
    return Client(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )

def init_db():
    config = get_config()['clickhouse']
    # 先连接默认数据库以创建新库
    client_default = Client(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database='default'
    )
    client_default.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
    
    client = get_ch_client()
    
    # 股票历史分钟级/日级表 (支持ReplacingMergeTree去重)
    # 按级别创建不同的表，使用 toYYYYMM(trade_time) 分区，避免分区过多
    levels = ['1m', '5m', '15m', '30m', '60m', 'daily']
    for level in levels:
        table_name = f"stock_history_{level}"
        if level == 'daily':
            partition_by = "toYYYYMM(trade_time)"
        else:
            partition_by = "toYYYYMM(trade_time)"
            
        client.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                stock_code String,
                trade_time DateTime('Asia/Shanghai'),
                open Decimal(18, 4),
                close Decimal(18, 4),
                high Decimal(18, 4),
                low Decimal(18, 4),
                volume Decimal(18, 4),
                amount Decimal(18, 4)
            ) ENGINE = ReplacingMergeTree()
            PARTITION BY {partition_by}
            ORDER BY (stock_code, trade_time)
        ''')
    
    # 财务数据表
    client.execute('''
        CREATE TABLE IF NOT EXISTS stock_finance (
            stock_code String,
            trade_date Date,
            close Decimal(18, 2),
            turnover_rate Decimal(18, 2),
            turnover_rate_f Decimal(18, 2),
            volume_ratio Decimal(18, 2),
            pe Decimal(18, 2),
            pe_ttm Decimal(18, 2),
            pe_ttm_percentile Decimal(18, 4),
            pb Decimal(18, 2),
            ps Decimal(18, 2),
            ps_ttm Decimal(18, 2),
            dv_ratio Decimal(18, 2),
            dv_ttm Decimal(18, 2),
            total_share Decimal(24, 2),
            float_share Decimal(24, 2),
            free_share Decimal(24, 2),
            total_mv Decimal(24, 2),
            circ_mv Decimal(24, 2)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (stock_code, trade_date)
    ''')
    
    # 实时分时表
    client.execute('''
        CREATE TABLE IF NOT EXISTS stock_realtime_minute (
            stock_code String,
            trade_time DateTime('Asia/Shanghai'),
            price Decimal(18, 4),
            volume Decimal(18, 4),
            amount Decimal(18, 4)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (stock_code, trade_time)
    ''')
    
    # 实时快照表
    client.execute('''
        CREATE TABLE IF NOT EXISTS stock_realtime_snapshot (
            stock_code String,
            snapshot_time DateTime('Asia/Shanghai'),
            price Decimal(18, 2),
            open Decimal(18, 2),
            high Decimal(18, 2),
            low Decimal(18, 2),
            volume Decimal(18, 2),
            amount Decimal(18, 2),
            buy_vols Array(Decimal(18, 2)),
            buy_prices Array(Decimal(18, 2)),
            sell_vols Array(Decimal(18, 2)),
            sell_prices Array(Decimal(18, 2))
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (stock_code, snapshot_time)
    ''')

    # WebSocket 推送表
    client.execute('''
        CREATE TABLE IF NOT EXISTS stock_ws_data (
            stock_code String,
            push_time DateTime('Asia/Shanghai'),
            data_type String,
            content String
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (stock_code, data_type, push_time)
    ''')
    
    print("ClickHouse 数据库表初始化完成！")

if __name__ == '__main__':
    # 注意：初次运行可能需要手动建库 CREATE DATABASE IF NOT EXISTS stock_data;
    init_db()
