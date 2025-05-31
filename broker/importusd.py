import json
import pyodbc

# تنظیمات اتصال به دیتابیس (بدون رمز)
server = 'localhost'
database = 'ime'
driver = 'ODBC Driver 17 for SQL Server'

conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# خواندن داده‌های json
with open('usd_daily_prices.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# وارد کردن داده‌ها به جدول
for date_shamsi, usd_price in data.items():
    cursor.execute(
        "INSERT INTO usd_prices (date_shamsi, usd_price) VALUES (?, ?)",
        date_shamsi, usd_price
    )

conn.commit()
cursor.close()
conn.close()

print("وارد کردن داده‌ها با موفقیت انجام شد.")