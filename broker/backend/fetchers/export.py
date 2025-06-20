import pyodbc
import requests
import jdatetime
import time
from datetime import timedelta

# تابع برای اتصال به دیتابیس
def connect_to_sql_server():
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost;'  # یا نام سرور شما
        'DATABASE=ime;'       # نام دیتابیس شما
        'Trusted_Connection=yes;'  # استفاده از Windows Authentication
    )
    return conn

# تابع برای تبدیل تاریخ شمسی به میلادی
def convert_to_gregorian(shamsi_date):
    try:
        if shamsi_date:
            shamsi_parts = shamsi_date.split('/')
            return jdatetime.date(int(shamsi_parts[0]), int(shamsi_parts[1]), int(shamsi_parts[2])).togregorian().strftime('%Y-%m-%d')
        return None
    except Exception as e:
        print(f"خطا در تبدیل تاریخ: {e}")
        return None

# تابع برای وارد کردن داده‌ها به جدول export
def insert_data_into_export(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO export (product_name, symbol, market, producer, contract_type, lowest_price, closing_price, highest_price, supply_volume, base_price, demand_volume, contract_volume, transaction_value, transaction_date, delivery_date, delivery_location, supplier, settlement_date, broker, supply_method, purchase_method, daily_settlement_rate, currency_type, unit, supply_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('GoodsName'), 
            item.get('Symbol'), 
            item.get('xRingName', 'N/A'), 
            item.get('ProducerName'), 
            item.get('ContractType'), 
            item.get('MinPrice'), 
            item.get('Price'), 
            item.get('MaxPrice'), 
            item.get('arze'), 
            item.get('BasePrice'), 
            item.get('taghaza'), 
            item.get('Quantity'), 
            item.get('TotalPrice'), 
            convert_to_gregorian(item['date']),  # تبدیل تاریخ معامله
            convert_to_gregorian(item['DeliveryDate']),  # تبدیل تاریخ تحویل
            item.get('Warehouse'), 
            item.get('ArzehKonandeh'), 
            None,  # در این داده‌ها تاریخ تسویه موجود نیست
            item.get('cBrokerSpcName'), 
            item.get('ModeDescription'), 
            item.get('MethodDescription'), 
            item.get('NerkhArz'), 
            item.get('Currency'), 
            item.get('Unit'), 
            item.get('arzehPk')
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای دریافت داده‌ها از URL مشخص‌شده
def fetch_data_from_url(start_date_shamsi, end_date_shamsi):
    url = f'https://www.ime.co.ir/subsystems/ime/fiziki/export.ashx?f={start_date_shamsi}&t={end_date_shamsi}&m=0&c=0&s=0&p=0&lang=8&order=asc&offset=0&limit=2000000000'
    
    try:
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json().get('rows', [])  # دریافت داده‌ها از بخش "rows"
            return data
        else:
            print(f"خطا در دریافت داده‌ها: {response.status_code}")
            return []
    except Exception as e:
        print(f"خطا در ارسال درخواست: {e}")
        return []

# تابع برای دریافت داده‌ها برای تعداد هفته مشخص
def fetch_data_for_days(start_date_shamsi, days_count):
    start_year, start_month, start_day = map(int, start_date_shamsi.split('/'))
    current_date_miladi = jdatetime.date(start_year, start_month, start_day).togregorian()

    for day in range(days_count):  # تعداد روزها از ورودی گرفته می‌شود
        # تبدیل تاریخ میلادی به شمسی
        start_date_shamsi = jdatetime.date.fromgregorian(date=current_date_miladi).strftime('%Y/%m/%d')
        end_date_shamsi = start_date_shamsi  # برای روزانه، تاریخ پایان همان تاریخ شروع است

        print(f"\nدر حال پردازش داده‌ها برای تاریخ {start_date_shamsi}...")

        # جمع‌آوری داده‌ها برای تاریخ مشخص‌شده
        data = fetch_data_from_url(start_date_shamsi, end_date_shamsi)

        print("در حال انتظار به مدت 5 ثانیه برای جمع‌آوری کامل داده‌ها...")
        time.sleep(5)

        if data:
            print(f"داده‌های دریافتی برای تاریخ {start_date_shamsi}:")
            insert_data_into_export(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

