import sys
sys.path.append('/app')

from app.db.mysql import get_mysql_connection

try:
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, name FROM tushare_stock_basic")
    rows = cursor.fetchall()
    
    with open('/app/stocks.csv', 'w', encoding='utf-8') as f:
        f.write("symbol,name\n")
        for row in rows:
            f.write(f"{row[0]},{row[1]}\n")
            
    print(f"Success, exported {len(rows)} stocks")
except Exception as e:
    print("Error:", e)
