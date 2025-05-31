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

# تابع برای وارد کردن داده‌ها به جدول options
def insert_data_into_options(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO options (contract, description, volume, value, highest_price, lowest_price, last_price, first_price, open_positions, active_clients_count, active_brokers_count, buyers_count, sellers_count, settlement_price, settlement_change_percentage, transaction_date, apply_date, individual_buy_value, individual_sell_value, legal_buy_value, legal_sell_value, individual_buy_volume, individual_sell_volume, legal_buy_volume, legal_sell_volume, last_day_closing_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('ContractCode'), 
            item.get('ContractDescription'), 
            item.get('TradesVolume'), 
            item.get('TradesValue'), 
            item.get('MaxPrice'), 
            item.get('MinPrice'), 
            item.get('LastPrice'), 
            item.get('FirstPrice'), 
            item.get('OpenInterest'), 
            item.get('ActiveCustomers'), 
            item.get('ActiveBrokers'), 
            item.get('C_Buy'), 
            item.get('C_Sell'), 
            item.get('TodaySettlementPrice'), 
            item.get('SettlementPricePercent'), 
            convert_to_gregorian(item['DT']),  # تاریخ معامله به میلادی
            convert_to_gregorian(item['DeliveryDate']),  # تاریخ تحویل به میلادی
            item.get('Val_Haghighi_Buy'), 
            item.get('Val_Haghighi_Sell'), 
            item.get('Val_Hoghooghi_Buy'), 
            item.get('Val_Hoghooghi_Sell'), 
            item.get('Vol_Haghighi_Buy'), 
            item.get('Vol_Haghighi_Sell'), 
            item.get('Vol_Hoghooghi_Buy'), 
            item.get('Vol_Hoghooghi_Sell'), 
            item.get('LastSettlementPrice')
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای دریافت داده‌ها از URL مشخص شده
def fetch_data_from_url(start_date_shamsi, end_date_shamsi):
    url = f'https://www.ime.co.ir/subsystems/ime/option/optionboarddata.ashx?f={start_date_shamsi}&t={end_date_shamsi}&c=-1&ot=0&lang=8&order=asc&offset=0&limit=2000000000'
    
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
            insert_data_into_options(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

