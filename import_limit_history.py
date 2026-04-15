import os
import pymysql
from clickhouse_driver import Client
import datetime
from tqdm import tqdm

MYSQL_HOST = "103.236.75.183"
MYSQL_USER = "stock_1m"
MYSQL_PASS = "SRtBGaDPTAewz2TP"
MYSQL_DB = "stock_1m"

CH_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "Clk_7k_4mN_wR3jX_2026")
CH_DB = os.getenv("CLICKHOUSE_DB", "stock_data")

# Fallback for local testing if needed
# CH_PORT = 9001 if using temp container, etc.

def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def get_ch_client():
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASS,
        database=CH_DB
    )

def transfer_table(mysql_table, ch_table, columns, batch_size=10000):
    print(f"Transferring {mysql_table} -> {ch_table}...")
    try:
        mysql_conn = get_mysql_conn()
        ch_client = get_ch_client()
        
        with mysql_conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {mysql_table}")
            total = cursor.fetchone()['cnt']
            
            if total == 0:
                print(f"No data in {mysql_table}")
                return
                
            cursor.execute(f"SELECT {', '.join(columns)} FROM {mysql_table}")
            
            pbar = tqdm(total=total)
            batch = []
            
            for row in cursor:
                # Format row for clickhouse
                formatted_row = []
                for col in columns:
                    val = row.get(col)
                    if isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
                        # Convert date to string or datetime for ClickHouse Date
                        val = val.strftime('%Y-%m-%d')
                    elif isinstance(val, datetime.timedelta):
                        # Convert timedelta (MySQL TIME) to string
                        val = str(val)
                    formatted_row.append(val)
                
                batch.append(tuple(formatted_row))
                
                if len(batch) >= batch_size:
                    ch_client.execute(f"INSERT INTO {ch_table} ({', '.join(columns)}) VALUES", batch)
                    pbar.update(len(batch))
                    batch = []
            
            if batch:
                ch_client.execute(f"INSERT INTO {ch_table} ({', '.join(columns)}) VALUES", batch)
                pbar.update(len(batch))
            
            pbar.close()
            print(f"Successfully transferred {total} rows for {mysql_table}")
            
    except Exception as e:
        print(f"Error transferring {mysql_table}: {e}")
    finally:
        if 'mysql_conn' in locals():
            mysql_conn.close()
        if 'ch_client' in locals():
            ch_client.disconnect()

if __name__ == "__main__":
    tables_map = [
        (
            "limit_up_records", 
            "stock_limit_up_records",
            ['trade_date', 'stock_code', 'stock_name', 'price', 'change_percent',
            'free_float_mktcap', 'first_limit_time', 'final_limit_time', 'limit_details',
            'consecutive_days', 'sealed_volume', 'sealed_amount', 'sealed_turnover_ratio',
            'sealed_flow_ratio', 'open_count', 'boards', 'limit_type', 'is_limit_up', 'reason_text']
        ),
        (
            "zhaban_records",
            "stock_zhaban_records",
            ['trade_date', 'stock_code', 'stock_name', 'price', 'change_percent',
            'first_limit_time', 'open_count', 'seal_duration', 'time_detail',
            'limit_up_price', 'ever_limit_up']
        ),
        (
            "limit_down_records",
            "stock_limit_down_records",
            ['trade_date', 'stock_code', 'stock_name', 'price', 'change_percent',
            'first_limit_time', 'final_limit_time', 'consecutive_days',
            'sealed_volume', 'sealed_amount', 'sealed_turnover_ratio', 'sealed_flow_ratio',
            'open_count', 'limit_type', 'is_limit_down', 'reason_text']
        ),
        (
            "ever_limit_down_records",
            "stock_ever_limit_down_records",
            ['trade_date', 'stock_code', 'stock_name', 'price', 'change_percent',
            'first_limit_time', 'open_count', 'seal_duration', 'time_detail',
            'limit_down_price', 'ever_limit_down']
        ),
        (
            "bidding_records",
            "stock_bidding_records",
            ['trade_date', 'stock_code', 'stock_name', 'current_price', 'change_percent',
            'bidding_match_price', 'bidding_anomaly_type', 'bidding_anomaly_desc', 'bidding_rating',
            'bidding_volume', 'bidding_amount', 'bidding_unmatched_volume', 'bidding_unmatched_amount',
            'bidding_change_percent']
        ),
        (
            "stock_board_records",
            "stock_board_records",
            ['stock_code', 'board_code', 'stock_name', 'board_name', 'stock_order', 'industry', 'tdx_industry', 'scrape_time', 'source_url']
        )
    ]
    
    for mysql_t, ch_t, cols in tables_map:
        transfer_table(mysql_t, ch_t, cols)
