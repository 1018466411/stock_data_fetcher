import yaml
import os
import logging
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

def get_api_key():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('API_KEY='):
                    return line.split('=', 1)[1].strip('"\'')
    return ""

def set_api_key(api_key):
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    lines = []
    found = False
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
    for i, line in enumerate(lines):
        if line.strip().startswith('API_KEY='):
            lines[i] = f'API_KEY="{api_key}"\n'
            found = True
            break
            
    if not found:
        lines.append(f'API_KEY="{api_key}"\n')
        
    with open(env_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def save_config(config):
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, sort_keys=False)

# Default schema definition
DEFAULT_SCHEMA = {
    'history_1m': {
        'fields': {
            'stock_code': 'String',
            'trade_time': 'DateTime(\'Asia/Shanghai\')',
            'open': 'Decimal(18, 4)',
            'close': 'Decimal(18, 4)',
            'high': 'Decimal(18, 4)',
            'low': 'Decimal(18, 4)',
            'volume': 'Decimal(18, 4)',
            'amount': 'Decimal(18, 4)'
        },
        'order_by': '(stock_code, trade_time)',
        'partition_by': 'toYYYYMM(trade_time)'
    },
    'history_5m': {
        'fields': {
            'stock_code': 'String',
            'trade_time': 'DateTime(\'Asia/Shanghai\')',
            'open': 'Decimal(18, 4)',
            'close': 'Decimal(18, 4)',
            'high': 'Decimal(18, 4)',
            'low': 'Decimal(18, 4)',
            'volume': 'Decimal(18, 4)',
            'amount': 'Decimal(18, 4)'
        },
        'order_by': '(stock_code, trade_time)',
        'partition_by': 'toYYYYMM(trade_time)'
    },
    'history_15m': {
        'fields': {
            'stock_code': 'String',
            'trade_time': 'DateTime(\'Asia/Shanghai\')',
            'open': 'Decimal(18, 4)',
            'close': 'Decimal(18, 4)',
            'high': 'Decimal(18, 4)',
            'low': 'Decimal(18, 4)',
            'volume': 'Decimal(18, 4)',
            'amount': 'Decimal(18, 4)'
        },
        'order_by': '(stock_code, trade_time)',
        'partition_by': 'toYYYYMM(trade_time)'
    },
    'history_30m': {
        'fields': {
            'stock_code': 'String',
            'trade_time': 'DateTime(\'Asia/Shanghai\')',
            'open': 'Decimal(18, 4)',
            'close': 'Decimal(18, 4)',
            'high': 'Decimal(18, 4)',
            'low': 'Decimal(18, 4)',
            'volume': 'Decimal(18, 4)',
            'amount': 'Decimal(18, 4)'
        },
        'order_by': '(stock_code, trade_time)',
        'partition_by': 'toYYYYMM(trade_time)'
    },
    'history_60m': {
        'fields': {
            'stock_code': 'String',
            'trade_time': 'DateTime(\'Asia/Shanghai\')',
            'open': 'Decimal(18, 4)',
            'close': 'Decimal(18, 4)',
            'high': 'Decimal(18, 4)',
            'low': 'Decimal(18, 4)',
            'volume': 'Decimal(18, 4)',
            'amount': 'Decimal(18, 4)'
        },
        'order_by': '(stock_code, trade_time)',
        'partition_by': 'toYYYYMM(trade_time)'
    },
    'history_daily': {
        'fields': {
            'stock_code': 'String',
            'trade_time': 'DateTime(\'Asia/Shanghai\')',
            'open': 'Decimal(18, 4)',
            'close': 'Decimal(18, 4)',
            'high': 'Decimal(18, 4)',
            'low': 'Decimal(18, 4)',
            'volume': 'Decimal(18, 4)',
            'amount': 'Decimal(18, 4)'
        },
        'order_by': '(stock_code, trade_time)',
        'partition_by': 'toYYYYMM(trade_time)'
    },
    'finance': {
        'fields': {
            'stock_code': 'String',
            'trade_date': 'Date',
            'close': 'Decimal(18, 2)',
            'turnover_rate': 'Decimal(18, 2)',
            'turnover_rate_f': 'Decimal(18, 2)',
            'volume_ratio': 'Decimal(18, 2)',
            'pe': 'Decimal(18, 2)',
            'pe_ttm': 'Decimal(18, 2)',
            'pe_ttm_percentile': 'Decimal(18, 4)',
            'pb': 'Decimal(18, 2)',
            'ps': 'Decimal(18, 2)',
            'ps_ttm': 'Decimal(18, 2)',
            'dv_ratio': 'Decimal(18, 2)',
            'dv_ttm': 'Decimal(18, 2)',
            'total_share': 'Decimal(24, 2)',
            'float_share': 'Decimal(24, 2)',
            'free_share': 'Decimal(24, 2)',
            'total_mv': 'Decimal(24, 2)',
            'circ_mv': 'Decimal(24, 2)'
        },
        'order_by': '(stock_code, trade_date)',
        'partition_by': None
    },
    'realtime_minute': {
        'fields': {
            'stock_code': 'String',
            'trade_time': 'DateTime(\'Asia/Shanghai\')',
            'price': 'Decimal(18, 4)',
            'volume': 'Decimal(18, 4)',
            'amount': 'Decimal(18, 4)'
        },
        'order_by': '(stock_code, trade_time)',
        'partition_by': None
    },
    'realtime_snapshot': {
        'fields': {
            'stock_code': 'String',
            'snapshot_time': 'DateTime(\'Asia/Shanghai\')',
            'price': 'Decimal(18, 2)',
            'open': 'Decimal(18, 2)',
            'high': 'Decimal(18, 2)',
            'low': 'Decimal(18, 2)',
            'volume': 'Decimal(18, 2)',
            'amount': 'Decimal(18, 2)',
            'buy_vols': 'Array(Decimal(18, 2))',
            'buy_prices': 'Array(Decimal(18, 2))',
            'sell_vols': 'Array(Decimal(18, 2))',
            'sell_prices': 'Array(Decimal(18, 2))'
        },
        'order_by': '(stock_code, snapshot_time)',
        'partition_by': None
    },
    'ws_data': {
        'fields': {
            'stock_code': 'String',
            'push_time': 'DateTime(\'Asia/Shanghai\')',
            'data_type': 'String',
            'content': 'String'
        },
        'order_by': '(stock_code, data_type, push_time)',
        'partition_by': None
    }
}

class BaseDB:
    def __init__(self, config):
        self.config = config
        self.tables_config = config.get('tables', {})

    def init_db(self):
        raise NotImplementedError

    def execute(self, query, with_column_types=False):
        raise NotImplementedError

    def insert(self, table_key, data_dicts):
        raise NotImplementedError

    def query(self, query, with_column_types=False):
        raise NotImplementedError

    def alter_table(self, old_config, new_config):
        raise NotImplementedError

class ClickHouseDB(BaseDB):
    def __init__(self, config):
        super().__init__(config)
        ch_config = config.get('clickhouse', {})
        self.client = Client(
            host=ch_config.get('host', 'localhost'),
            port=ch_config.get('port', 9000),
            user=ch_config.get('user', 'default'),
            password=ch_config.get('password', ''),
            database=ch_config.get('database', 'stock_data')
        )
        self.client_default = Client(
            host=ch_config.get('host', 'localhost'),
            port=ch_config.get('port', 9000),
            user=ch_config.get('user', 'default'),
            password=ch_config.get('password', ''),
            database='default'
        )
        self.db_name = ch_config.get('database', 'stock_data')

    def init_db(self):
        self.client_default.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        
        for table_key, schema in DEFAULT_SCHEMA.items():
            table_config = self.tables_config.get(table_key, {})
            table_name = table_config.get('name', f'stock_{table_key}')
            fields_mapping = table_config.get('fields', {})
            
            columns_def = []
            for base_field, field_type in schema['fields'].items():
                db_field = fields_mapping.get(base_field, base_field)
                columns_def.append(f"{db_field} {field_type}")
            
            columns_str = ",\n                ".join(columns_def)
            
            # map order_by fields
            order_by = schema['order_by']
            for base_field in schema['fields'].keys():
                db_field = fields_mapping.get(base_field, base_field)
                order_by = order_by.replace(base_field, db_field)
                
            partition_str = ""
            if schema['partition_by']:
                partition_by = schema['partition_by']
                for base_field in schema['fields'].keys():
                    db_field = fields_mapping.get(base_field, base_field)
                    partition_by = partition_by.replace(base_field, db_field)
                partition_str = f"PARTITION BY {partition_by}"
                
            sql = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns_str}
                ) ENGINE = ReplacingMergeTree()
                {partition_str}
                ORDER BY {order_by}
            '''
            self.client.execute(sql)
        print("ClickHouse 数据库表初始化完成！")

    def execute(self, query, with_column_types=False):
        return self.client.execute(query, with_column_types=with_column_types)

    def query(self, query, with_column_types=False):
        return self.client.execute(query, with_column_types=with_column_types)

    def insert(self, table_key, data_dicts):
        if not data_dicts:
            return
            
        table_config = self.tables_config.get(table_key)
        if not table_config:
            raise ValueError(f"Table config for {table_key} not found")
            
        table_name = table_config['name']
        fields_mapping = table_config['fields']
        
        # Get base fields defined in default schema to maintain order
        base_fields = list(DEFAULT_SCHEMA[table_key]['fields'].keys())
        db_fields = [fields_mapping.get(f, f) for f in base_fields]
        
        tuples_data = []
        for row in data_dicts:
            tuple_row = tuple(row.get(base_field) for base_field in base_fields)
            tuples_data.append(tuple_row)
            
        fields_str = ", ".join(db_fields)
        self.client.execute(f"INSERT INTO {table_name} ({fields_str}) VALUES", tuples_data)

    def alter_table(self, table_key, old_table_config, new_table_config):
        old_name = old_table_config.get('name')
        new_name = new_table_config.get('name')
        
        # 1. Rename table if changed
        if old_name != new_name:
            # Check if old table exists
            res = self.client.execute(f"EXISTS TABLE {old_name}")
            if res and res[0][0] == 1:
                self.client.execute(f"RENAME TABLE {old_name} TO {new_name}")
                
        # 2. Rename columns if changed
        old_fields = old_table_config.get('fields', {})
        new_fields = new_table_config.get('fields', {})
        
        res = self.client.execute(f"EXISTS TABLE {new_name}")
        if res and res[0][0] == 1:
            for base_field, old_db_field in old_fields.items():
                new_db_field = new_fields.get(base_field)
                if old_db_field != new_db_field:
                    # Rename column in ClickHouse
                    try:
                        self.client.execute(f"ALTER TABLE {new_name} RENAME COLUMN {old_db_field} TO {new_db_field}")
                        logger.info(f"Renamed column {old_db_field} to {new_db_field} in {new_name}")
                    except Exception as e:
                        logger.error(f"Failed to rename column {old_db_field} to {new_db_field}: {e}")

_db_instance = None

def get_db():
    global _db_instance
    if _db_instance is None:
        config = get_config()
        db_type = config.get('db_type', 'clickhouse')
        if db_type == 'clickhouse':
            _db_instance = ClickHouseDB(config)
        else:
            raise ValueError(f"Unsupported db_type: {db_type}")
    else:
        # Update config in case it changed
        _db_instance.config = get_config()
        _db_instance.tables_config = _db_instance.config.get('tables', {})
    return _db_instance

def get_ch_client():
    # Deprecated, keeping for backward compatibility if missed any, but shouldn't be used
    return get_db().client

def init_db():
    db = get_db()
    db.init_db()

if __name__ == '__main__':
    init_db()
