import pyodbc
import jdatetime
from datetime import timedelta
import time
import json
import requests

# تابع برای اتصال به دیتابیس
def connect_to_sql_server():
    # تنظیمات اتصال به SQL Server با Windows Authentication
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost;'  # یا نام سرور شما اگر محلی نیست
        'DATABASE=ime;'       # نام دیتابیس شما
        'Trusted_Connection=yes;'  # استفاده از Windows Authentication
    )
    return conn

# تبدیل تاریخ شمسی به میلادی
def convert_to_gregorian(shamsi_date):
    try:
        # اگر تاریخ به صورت شمسی بود آن را به میلادی تبدیل کنید
        if shamsi_date:
            shamsi_parts = shamsi_date.split('/')
            return jdatetime.date(int(shamsi_parts[0]), int(shamsi_parts[1]), int(shamsi_parts[2])).togregorian().strftime('%Y-%m-%d')
        return None
    except Exception as e:
        print(f"خطا در تبدیل تاریخ: {e}")
        return None

# در تابع insert_data_into_kala
def insert_data_into_kala(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO kala (product_name, symbol, market, producer, contract_type, weighted_average_price, transaction_value, closing_price, lowest_price, highest_price, base_price, supply_volume, demand_volume, contract_volume, unit, transaction_date, delivery_date, delivery_location, supplier, settlement_date, broker, supply_method, purchase_method, packaging, settlement_type, currency, supply_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item['GoodsName'], 
            item['Symbol'], 
            item.get('Talar', 'N/A'), 
            item['ProducerName'], 
            item['ContractType'], 
            item.get('Price', 0),
            item.get('TotalPrice', 0), 
            item.get('Price1', 0), 
            item.get('MinPrice', 0), 
            item.get('MaxPrice', 0), 
            item.get('ArzeBasePrice', 0),
            item['arze'], 
            item['taghaza'], 
            item.get('Quantity', 0), 
            item['Unit'], 
            convert_to_gregorian(item['date']),  # تبدیل تاریخ معامله
            convert_to_gregorian(item['DeliveryDate']),  # تبدیل تاریخ تحویل
            item['Warehouse'],
            item['ArzehKonandeh'], 
            convert_to_gregorian(item['SettlementDate']),  # تبدیل تاریخ تسویه
            item['cBrokerSpcName'], 
            item['ModeDescription'], 
            item['MethodDescription'], 
            item['PacketName'], 
            item['Tasvieh'], 
            item['Currency'], 
            item['arzehPk']
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای ارسال درخواست به API و دریافت داده‌ها
def fetch_data_from_api(start_date_shamsi, end_date_shamsi):
    url = 'https://www.ime.co.ir/subsystems/ime/services/home/imedata.asmx/GetAmareMoamelatList'
    
    # `payload` شامل تاریخ‌ها به فرمت شمسی
    payload = {
        "Language": 8,
        "fari": False,
        "GregorianFromDate": start_date_shamsi,  # تاریخ شروع به شمسی
        "GregorianToDate": end_date_shamsi,      # تاریخ پایان به شمسی
        "MainCat": 0,
        "Cat": 0,
        "SubCat": 0,
        "Producer": 0
    }

    headers = {'Content-Type': 'application/json'}

    try:
        # ارسال درخواست به سرور
        response = requests.post(url, json=payload, headers=headers)

        # بررسی وضعیت درخواست
        if response.status_code == 200:
            # تبدیل پاسخ به JSON
            data = response.json().get('d', '[]')  # استخراج داده‌ها از کلید 'd'
            return json.loads(data)  # تبدیل رشته JSON به لیست دیکشنری‌ها
        else:
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
        data = fetch_data_from_api(start_date_shamsi, end_date_shamsi)

        print("در حال انتظار به مدت 5 ثانیه برای جمع‌آوری کامل داده‌ها...")
        time.sleep(5)

        if data:
            print(f"داده‌های دریافتی برای تاریخ {start_date_shamsi}:")
            insert_data_into_kala(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

