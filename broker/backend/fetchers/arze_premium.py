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

# تابع برای دریافت نام تمام ستون‌های یک جدول
def get_table_columns(table_name):
    conn = connect_to_sql_server()
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        """
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"خطا در دریافت ستون‌های جدول {table_name}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# تابع برای حذف سطرهای تکراری از جدول با معیار تمام ستون‌ها
def remove_duplicate_rows(table_name):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    try:
        print(f"در حال حذف سطرهای تکراری از جدول {table_name}...")

        # دریافت نام تمام ستون‌های جدول
        columns = get_table_columns(table_name)
        if not columns:
            print(f"خطا: هیچ ستونی برای جدول {table_name} یافت نشد.")
            return

        # ایجاد لیست ستون‌ها به صورت کاما جدا
        column_list = ", ".join(columns)

        # کوئری حذف سطرهای تکراری
        query = f"""
        WITH CTE AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {column_list} ORDER BY (SELECT NULL)) AS row_num
            FROM {table_name}
        )
        DELETE FROM CTE WHERE row_num > 1;
        """
        cursor.execute(query)
        conn.commit()
        print(f"سطرهای تکراری از جدول {table_name} حذف شدند.")
    except Exception as e:
        print(f"خطایی در حذف سطرهای تکراری از جدول {table_name} رخ داد: {e}")
    finally:
        cursor.close()
        conn.close()

# تابع برای درج داده‌ها در جدول arze_premium
def insert_data_into_arze_premium(data):
    conn = connect_to_sql_server()
    cursor = conn.cursor()

    for item in data:
        cursor.execute('''
            INSERT INTO arze_premium (
                Attachment, bArzehRadifNamadKala, xKalaNamadKala, bArzehRadifArzeh, xContractKindSharh,
                transaction_date, bArzehRadifPremium, bArzehRadifTasviehTypeSharh, PremiumTasviehDate,
                bArzehRadifFinalContractPrepayment, bArzehRadifExtraBasePriceRefrence,
                bArzehRadifExtraFinalContractFormula, PrePaymentFinalContract, PremiumPrice,
                FinalContractType, bArzehRadifTarTahvil, bArzehRadifExtraFinalPremiumContractDate,
                FinalContractTasviehType, FinalContractRate, bArzehRadifMinMahmooleh, cBrokerSpcName,
                bArzehRadifMaxMahmooleh, xTolidKonandehSharh, ArzehKonandeh, xMahalTahvilSharh,
                bArzehRadifShekl, bArzehRadifArzehSharh, bArzehRadifMinBasePrice, bArzehRadifMaxBasePrice,
                bArzehRadifMaxKharidM, bArzehRadifKashfNerkhMinBuy, bArzehRadifKashfNerkhMinBuy1,
                bArzehRadifMaxMahmooleh1, bArzehRadifMojazTahvilMinTel, ArzePK
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('Attachment'),
            item.get('bArzehRadifNamadKala'),
            item.get('xKalaNamadKala'),
            item.get('bArzehRadifArzeh'),
            item.get('xContractKindSharh'),
            item.get('bArzehTarArzeh'),
            item.get('bArzehRadifPremium'),
            item.get('bArzehRadifTasviehTypeSharh'),
            item.get('PremiumTasviehDate'),
            item.get('bArzehRadifFinalContractPrepayment'),
            item.get('bArzehRadifExtraBasePriceRefrence'),
            item.get('bArzehRadifExtraFinalContractFormula'),
            item.get('PrePaymentFinalContract'),
            item.get('PremiumPrice'),
            item.get('FinalContractType'),
            item.get('bArzehRadifTarTahvil'),
            item.get('bArzehRadifExtraFinalPremiumContractDate'),
            item.get('FinalContractTasviehType'),
            item.get('FinalContractRate'),
            item.get('bArzehRadifMinMahmooleh'),
            item.get('cBrokerSpcName'),
            item.get('bArzehRadifMaxMahmooleh'),
            item.get('xTolidKonandehSharh'),
            item.get('ArzehKonandeh'),
            item.get('xMahalTahvilSharh'),
            item.get('bArzehRadifShekl'),
            item.get('bArzehRadifArzehSharh'),
            item.get('bArzehRadifMinBasePrice'),
            item.get('bArzehRadifMaxBasePrice'),
            item.get('bArzehRadifMaxKharidM'),
            item.get('bArzehRadifKashfNerkhMinBuy'),
            item.get('bArzehRadifKashfNerkhMinBuy1'),
            item.get('bArzehRadifMaxMahmooleh1'),
            item.get('bArzehRadifMojazTahvilMinTel'),
            item.get('ArzePK')
        ))

    conn.commit()
    cursor.close()
    conn.close()

# تابع برای دریافت داده‌ها از API برای یک بازه زمانی مشخص
def fetch_data_from_api(start_date, end_date):
    url = "https://www.ime.co.ir/subsystems/ime/services/home/imedata.asmx/GetPremiumArzeList"

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
            insert_data_into_arze_premium(data)
        else:
            print(f"هیچ داده‌ای برای تاریخ {start_date_shamsi} موجود نیست.")

        time.sleep(2)
        current_date_miladi += timedelta(days=1)  # به روز بعد برو

