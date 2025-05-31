import pyodbc
import json
import requests
import jdatetime
from datetime import timedelta
import time

# تابع برای اتصال به دیتابیس SQL Server
def connect_to_sql_server():
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost;'  # نام سرور SQL Server
        'DATABASE=ime;'       # نام دیتابیس
        'Trusted_Connection=yes;'
    )
    return conn

# تابع برای درج داده‌ها در جدول arze_kala
def insert_data_into_arze_kala(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO arze_kala (
                Attachment, bArzehRadifPK, cBrokerSpcName, transaction_date, bArzehRadifNamadKala,
                bArzehRadifShekl, bArzehRadifSize, xTolidKonandehSharh, xMahalTahvilSharh,
                bArzehRadifArzeh, bArzehRadifMab, xContractKindSharh, bArzehRadifTarTahvil,
                bArzehRadifArzehSharh, bArzehRadifMaxMahmooleh, bArzehRadifZaribMahmooleh,
                bArzehRadifMinMahmooleh, bArzehRadifSumBuyOrders, bArzehRadifStatusSharh,
                bArzehRadifMinMab, bArzehRadifMaxMab, bArzehRadifDarsad, bArzehRadifMinArzeh,
                bArzehRadifKashfNerkhMinBuy, bArzehRadifMinTakhsis, bArzehRadifMojazTahvilMinTel,
                bArzehRadifVahedAndazegiri, bArzehRadifAndazehMahmuleh, bArzehRadifTasviehTypeID,
                bArzehRadifTasviehTypeSharh, bArzehRadifTikSize, bArzehRadifArzehAvalieh,
                bArzehRadifMaxBasePrice, ArzehKonandeh, xKalaNamTejari, xKalaNamadKala,
                ArzeshArzeh, bArzehRadifMaxKharidM, bArzehRadifTedadMahmooleh, xTolidKonandehPK,
                xKala_xGrouhAsliKalaPK, xKala_xGrouhKalaPK, xKala_xZirGrouhKalaPK, bArzehRadifMode,
                bArzehRadifBuyMethod, ModeDescription, MethodDescription, Currency, Unit, Talar
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('Attachment'),
            item.get('bArzehRadifPK'),
            item.get('cBrokerSpcName'),
            item.get('bArzehTarArzeh'),
            item.get('bArzehRadifNamadKala'),
            item.get('bArzehRadifShekl'),
            item.get('bArzehRadifSize'),
            item.get('xTolidKonandehSharh'),
            item.get('xMahalTahvilSharh'),
            item.get('bArzehRadifArzeh'),
            item.get('bArzehRadifMab'),
            item.get('xContractKindSharh'),
            item.get('bArzehRadifTarTahvil'),
            item.get('bArzehRadifArzehSharh'),
            item.get('bArzehRadifMaxMahmooleh'),
            item.get('bArzehRadifZaribMahmooleh'),
            item.get('bArzehRadifMinMahmooleh'),
            item.get('bArzehRadifSumBuyOrders'),
            item.get('bArzehRadifStatusSharh'),
            item.get('bArzehRadifMinMab'),
            item.get('bArzehRadifMaxMab'),
            item.get('bArzehRadifDarsad'),
            item.get('bArzehRadifMinArzeh'),
            item.get('bArzehRadifKashfNerkhMinBuy'),
            item.get('bArzehRadifMinTakhsis'),
            item.get('bArzehRadifMojazTahvilMinTel'),
            item.get('bArzehRadifVahedAndazegiri'),
            item.get('bArzehRadifAndazehMahmuleh'),
            item.get('bArzehRadifTasviehTypeID'),
            item.get('bArzehRadifTasviehTypeSharh'),
            item.get('bArzehRadifTikSize'),
            item.get('bArzehRadifArzehAvalieh'),
            item.get('bArzehRadifMaxBasePrice'),
            item.get('ArzehKonandeh'),
            item.get('xKalaNamTejari'),
            item.get('xKalaNamadKala'),
            item.get('ArzeshArzeh'),
            item.get('bArzehRadifMaxKharidM'),
            item.get('bArzehRadifTedadMahmooleh'),
            item.get('xTolidKonandehPK'),
            item.get('xKala_xGrouhAsliKalaPK'),
            item.get('xKala_xGrouhKalaPK'),
            item.get('xKala_xZirGrouhKalaPK'),
            item.get('bArzehRadifMode'),
            item.get('bArzehRadifBuyMethod'),
            item.get('ModeDescription'),
            item.get('MethodDescription'),
            item.get('Currency'),
            item.get('Unit'),
            item.get('Talar')
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای دریافت داده‌ها از API برای یک بازه زمانی مشخص
def fetch_data_from_api(start_date, end_date):
    url = f'https://www.ime.co.ir/subsystems/ime/auction/auction.ashx?fr=false&f={start_date}&t={end_date}&m=0&c=0&s=0&p=0&lang=8&order=asc&offset=0&limit=20'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get('rows', [])
            return data
        else:
            print("خطا در دریافت داده‌ها از API")
            return []
    except Exception as e:
        print(f"خطا در ارسال درخواست: {e}")
        return []

# تابع اصلی برای دریافت داده‌ها به‌صورت هفتگی
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
            insert_data_into_arze_kala(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

