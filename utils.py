import requests
import time
import logging
from db import get_config
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = get_config()
API_DOMAIN = config['api']['domain'].rstrip('/')

# 限速控制全局变量
RATE_LIMIT = 300  # 默认 300 次/分钟
_request_timestamps = []
_lock = threading.Lock()

def _check_rate_limit():
    """检查并控制请求频率"""
    global _request_timestamps
    with _lock:
        now = time.time()
        # 清理一分钟之前的记录
        _request_timestamps = [t for t in _request_timestamps if now - t < 60]
        
        if len(_request_timestamps) >= RATE_LIMIT:
            # 如果达到限制，计算需要等待的时间
            oldest_req = _request_timestamps[0]
            sleep_time = 60 - (now - oldest_req)
            if sleep_time > 0:
                logging.debug(f"达到每分钟 {RATE_LIMIT} 次请求限制，主动等待 {sleep_time:.2f} 秒...")
                time.sleep(sleep_time)
            # 等待后重新记录当前时间
            _request_timestamps.append(time.time())
        else:
            _request_timestamps.append(now)

def make_request(endpoint, params=None, method='GET'):
    if not params:
        params = {}

    api_key = config.get('api', {}).get('api_key') or __import__('db').get_api_key()
    if not api_key:
        logging.error("未配置 API Key，程序退出。")
        import os
        os._exit(1)

    headers = {'apiKey': api_key}
    url = f"{API_DOMAIN}{endpoint}"

    while True:
        try:
            _check_rate_limit()
            
            if method.upper() == 'POST':
                response = requests.post(url, json=params, headers=headers, timeout=60)
            else:
                response = requests.get(url, params=params, headers=headers, timeout=60)
            
            if response.status_code in [401, 402, 403]:
                logging.error(f"API Key 验证失败或无权限 (HTTP {response.status_code})，程序退出。URL: {url}")
                import os
                os._exit(1)

            # 如果触发限速 (假设 HTTP 状态码 429 或 错误码)
            if response.status_code == 429:
                logging.warning(f"触发限速，等待 5 秒后重试... URL: {url}")
                time.sleep(5)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') in [401, 402, 403]:
                logging.error(f"API Key 验证失败或无权限 (Code {data.get('code')})，程序退出。URL: {url}")
                import os
                os._exit(1)

            # 根据约定的业务 code 判断，这里假设非 200 为限速或其他错误
            if data.get('code') == 429 or '限速' in str(data.get('msg', '')):
                logging.warning(f"触发接口限速规则，等待 5 秒后重试... URL: {url}")
                time.sleep(5)
                continue
                
            if data.get('code') != 200:
                logging.error(f"接口返回错误: {data.get('msg')}")
                # 根据需要抛出异常或返回 None
                return None
                
            return data.get('data')
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None and e.response.status_code in [401, 402, 403]:
                logging.error(f"API Key 验证失败或无权限 (HTTP {e.response.status_code})，程序退出。URL: {url}")
                import os
                os._exit(1)
                
            trace_id = ""
            if 'response' in locals() and hasattr(response, 'headers'):
                trace_id = response.headers.get('x-trace-id', response.headers.get('trace-id', ''))
            
            error_msg = str(e)
            if 'SSLZeroReturnError' in error_msg or 'ConnectionResetError' in error_msg:
                logging.warning(f"SSL/连接中断(可能被服务端断开) TraceID: {trace_id}，等待 2 秒后重试... URL: {url}")
                time.sleep(2)
            else:
                logging.error(f"请求异常: {error_msg} TraceID: {trace_id}，等待 5 秒后重试... URL: {url}")
                time.sleep(5)