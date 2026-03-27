from fetch_history import fetch_and_save_finance, get_stock_list, make_request
from db import init_db
init_db()

try:
    print("Fetching finance data...")
    data = make_request("/api/stock/finance", {'stock_code': '000001.SZ', 'start_time': '2024-12-01', 'end_time': '2024-12-05', 'page': 0, 'page_size': 100}, method='POST')
    print("Data keys:", data.keys() if data else data)
    print("Records len:", len(data['list']) if data and 'list' in data else 0)
    
    # fetch_and_save_finance('000001.SZ', '2024-12-01', '2024-12-05')
    print("Done")
except Exception as e:
    print("Error:", e)
