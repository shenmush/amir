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

# تابع برای درج داده‌ها در جدول arze_export
def insert_data_into_arze_export(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO arze_export (
                Attachment, transaction_date, bArzehRadifNamadKala, bArzehRadifShekl, bArzehRadifSize,
                xTolidKonandehSharh, xMahalTahvilSharh, bArzehRadifArzeh, bArzehRadifMab,
                xContractKindSharh, bArzehRadifTarTahvil, cBrokerSpcName, bArzehRadifNooTahvil,
                bArzehRadifZamanTahvil, bArzehRadifBazarHadaf, bArzehRadifMinTedad, bArzehRadifNooPardakht,
                bArzehRadifNamBank, bArzehRadifArzehSharh, Symbol, ModeDescription, MethodDescription,
                Currency, Unit, bArzehRadifPK, ArzehKonandeh, MinMahmooleh, MaxOffer, Talar,
                bArzehRadifDarsad
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('Attachment'),
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
            item.get('cBrokerSpcName'),
            item.get('bArzehRadifNooTahvil'),
            item.get('bArzehRadifZamanTahvil'),
            item.get('bArzehRadifBazarHadaf'),
            item.get('bArzehRadifMinTedad'),
            item.get('bArzehRadifNooPardakht'),
            item.get('bArzehRadifNamBank'),
            item.get('bArzehRadifArzehSharh'),
            item.get('Symbol'),
            item.get('ModeDescription'),
            item.get('MethodDescription'),
            item.get('Currency'),
            item.get('Unit'),
            item.get('bArzehRadifPK'),
            item.get('ArzehKonandeh'),
            item.get('MinMahmooleh'),
            item.get('MaxOffer'),
            item.get('Talar'),
            item.get('bArzehRadifDarsad')
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای دریافت داده‌ها از API برای یک بازه زمانی مشخص
def fetch_data_from_api(start_date, end_date):
    url = "https://www.ime.co.ir/subsystems/ime/services/home/imedata.asmx/GetArzeSaderatiList"

    payload = {
        "Language": 8,
        "fari": False,
        "GregorianFromDate": start_date,
        "GregorianToDate": end_date,
        "MainCat": 0,
        "Cat": 0,
        "SubCat": 0,
        "Producer": 0
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json().get('d', '[]')
            return json.loads(data)
        else:
            print("خطا در دریافت داده‌ها")
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
            insert_data_into_arze_export(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

