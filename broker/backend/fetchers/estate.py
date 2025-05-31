import pyodbc
import requests
import jdatetime
import time
from datetime import timedelta
import json

# تابع برای اتصال به دیتابیس SQL Server
def connect_to_sql_server():
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost;'  # نام سرور شما
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

# تابع برای وارد کردن داده‌ها به جدول estate
def insert_data_into_estate(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO estate (product_name, symbol, market, producer, contract_type, supply_volume, base_price, demand_volume, contract_volume, unit, transaction_date, delivery_date, delivery_location, supplier, settlement_date, broker, supply_method, purchase_method, packaging, settlement_type, currency, supply_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('GoodsName'), 
            item.get('Symbol'), 
            item.get('Talar'),  # تالار
            item.get('ProducerName'), 
            item.get('ContractType'), 
            item.get('arze'),  # حجم عرضه
            item.get('ArzeBasePrice'),  # قیمت پایه عرضه
            item.get('taghaza'),  # تقاضا
            item.get('Quantity'),  # حجم قرارداد
            item.get('Unit'),  # واحد
            convert_to_gregorian(item['date']),  # تاریخ معامله
            convert_to_gregorian(item['DeliveryDate']),  # تاریخ تحویل
            item.get('Warehouse'),  # مکان تحویل
            item.get('ArzehKonandeh'),  # عرضه کننده
            convert_to_gregorian(item.get('SettlementDate')),  # تاریخ تسویه
            item.get('cBrokerSpcName'),  # کارگزار
            item.get('ModeDescription'),  # نحوه عرضه
            item.get('MethodDescription'),  # روش خرید
            item.get('PacketName'),  # نوع بسته‌بندی
            item.get('Tasvieh'),  # نوع تسویه
            item.get('Currency'),  # نوع ارز
            item.get('arzehPk')  # کد عرضه
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای دریافت داده‌ها از URL مشخص‌شده با استفاده از POST
def fetch_data_from_url(start_date_shamsi, end_date_shamsi):
    url = "https://www.ime.co.ir/subsystems/ime/services/home/imedata.asmx/GetAmareMoamelatList"
    
    # ساختار payload برای ارسال درخواست POST
    payload = {
        "Language": 8,
        "fari": True,
        "GregorianFromDate": start_date_shamsi,  # تاریخ شروع به شمسی
        "GregorianToDate": end_date_shamsi,      # تاریخ پایان به شمسی
        "MainCat": 6,  # دسته‌بندی املاک
        "Cat": 0,
        "SubCat": 0,
        "Producer": 0
    }

    headers = {
        'Content-Type': 'application/json',  # نوع محتوا
    }

    try:
        # ارسال درخواست POST
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            # داده‌ها به صورت JSON دریافت می‌شوند
            raw_data = response.json().get('d', '[]')  # داده‌ها از کلید 'd' استخراج می‌شوند
            data = json.loads(raw_data)  # تبدیل رشته JSON به لیست دیکشنری‌ها
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
            insert_data_into_estate(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

