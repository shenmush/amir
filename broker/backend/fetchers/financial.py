import pyodbc
import requests
import time
from datetime import timedelta
import jdatetime

# تابع برای اتصال به دیتابیس SQL Server
def connect_to_sql_server():
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost;'  # نام سرور شما
        'DATABASE=ime;'       # نام دیتابیس شما
        'Trusted_Connection=yes;'  # استفاده از Windows Authentication
    )
    return conn

# تابع برای تبدیل تاریخ به فرمت صحیح (اگر نیاز به تبدیل داشته باشیم)
def convert_to_date(date_string):
    try:
        if date_string:
            return jdatetime.date(int(date_string.split('/')[0]), int(date_string.split('/')[1]), int(date_string.split('/')[2])).togregorian().strftime('%Y-%m-%d')
        return None
    except Exception as e:
        print(f"خطا در تبدیل تاریخ: {e}")
        return None

# تابع برای وارد کردن داده‌ها به جدول financial
def insert_data_into_financial(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO financial (
                symbol, transaction_date, description, closing_price, 
                last_price, trade_count, volume, transaction_value, 
                lowest_price, highest_price, previous_price, 
                price_change_last, price_change_last_percent, 
                price_change_closing, price_change_closing_percent, lval18afc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('Namad'), 
            convert_to_date(item.get('DT')),  # تاریخ معامله
            item.get('NamadDescription'),  # شرح نماد
            item.get('PClosing'),  # قیمت پایانی
            item.get('PDrCotVal'),  # قیمت آخرین معامله
            item.get('ZTotTran'),  # تعداد معاملات
            item.get('QTotTran5J'),  # حجم
            item.get('QTotCap'),  # ارزش معاملاتی
            item.get('PriceMin'),  # کمترین قیمت
            item.get('PriceMax'),  # بیشترین قیمت
            item.get('PriceYesterday'),  # قیمت دیروز
            item.get('LastTradeChangePrice'),  # تغییر قیمت آخرین معامله
            item.get('LastTradeChangePricePercent'),  # درصد تغییر قیمت آخرین معامله
            item.get('LastPriceChangePrice'),  # تغییر قیمت پایانی
            item.get('LastPriceChangePricePercent'),  # درصد تغییر قیمت پایانی
            item.get('LVal18AFC')  # عنوان فارسی نماد
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای دریافت داده‌ها از URL مشخص‌شده
def fetch_data_from_url(start_date, end_date):
    url = f'https://www.ime.co.ir/subsystems/ime/bazaremali/bazaremalidata.ashx?f={start_date}&t={end_date}&c=ALL&ot=ALL&lang=8&order=asc&offset=0&limit=20000000'

    try:
        response = requests.get(url)

        if response.status_code == 200:
            # داده‌ها به صورت JSON دریافت می‌شوند
            data = response.json().get('rows', [])  # داده‌ها از کلید 'rows' استخراج می‌شوند
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
            insert_data_into_financial(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

