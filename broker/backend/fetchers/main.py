import arze_premium
import arze_kala
import arze_export
import options
import financial
import export
import estate
import future
import kala
from sqlalchemy import create_engine, text
import pandas as pd
import jdatetime
import time
import schedule  # کتابخانه زمان‌بندی برای اجرای خودکار

# اتصال به SQL Server
def connect_to_sql_server():
    engine = create_engine(
        "mssql+pyodbc://localhost/ime?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return engine

# دریافت داده‌ها از جدول
def fetch_data_from_table(table_name):
    engine = connect_to_sql_server()
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

# حذف سطرهای تکراری از جدول
def remove_duplicates_keep_one(table_name):
    print(f"\nشروع فرآیند شناسایی و حذف سطرهای تکراری از جدول '{table_name}'...")
    
    # دریافت داده‌ها از جدول
    df = fetch_data_from_table(table_name)
    
    # شناسایی سطرهای تکراری
    print("شناسایی سطرهای تکراری...")
    duplicate_rows = df[df.duplicated(subset=df.columns.drop('id'), keep='first')]
    print(f"تعداد سطرهای تکراری شناسایی شده: {len(duplicate_rows)}")
    
    if duplicate_rows.empty:
        print(f"هیچ سطر تکراری در جدول '{table_name}' یافت نشد.")
        return
    
    # شناسایی id‌هایی که باید حذف شوند
    ids_to_delete = duplicate_rows['id'].tolist()
    print(f"لیست id سطرهایی که باید حذف شوند: {ids_to_delete}")
    
    # حذف سطرها از جدول به صورت دونه‌دونه
    print("حذف سطرهای تکراری از جدول...")
    engine = connect_to_sql_server()
    with engine.connect() as conn:
        transaction = conn.begin()  # شروع تراکنش
        try:
            for single_id in ids_to_delete:
                conn.execute(
                    text(f"DELETE FROM {table_name} WHERE id = :id"),
                    {"id": single_id}
                )
            transaction.commit()  # ذخیره تغییرات
            print(f"سطرهای تکراری از جدول '{table_name}' حذف شدند.")
        except Exception as e:
            transaction.rollback()  # بازگرداندن تغییرات در صورت خطا
            print(f"خطا در حذف داده‌ها از جدول '{table_name}': {e}")

# اجرای وظیفه برای جمع‌آوری داده‌های روزانه
def run_daily_task():
    print("\nشروع جمع‌آوری داده‌های روزانه...")
    
    # تاریخ امروز به فرمت شمسی
    start_date = jdatetime.date.today().strftime('%Y/%m/%d')
    days_count = 1  # تعداد روزها فقط یک روز

    print(f"تاریخ شروع: {start_date}")
    print("شروع اجرای برنامه‌ها برای داده‌های مختلف...\n")

    # اجرای هر برنامه و حذف سطرهای تکراری
    try:
        tables_and_modules = [
            ('arze_premium', arze_premium),
            ('arze_kala', arze_kala),
            ('arze_export', arze_export),
            ('options', options),
            ('financial', financial),
            ('export', export),
            ('estate', estate),
            ('future', future),
            ('kala', kala)
        ]

        for table_name, module in tables_and_modules:
            print(f"در حال اجرای برنامه {table_name}...")
            module.fetch_data_for_days(start_date, days_count)  # استفاده از تابع روزانه
            print(f"پایان برنامه {table_name}.\n")

            # حذف سطرهای تکراری از جدول
            remove_duplicates_keep_one(table_name)

        print("تمام برنامه‌ها با موفقیت اجرا شدند.")
    except Exception as e:
        print(f"خطایی رخ داده است: {e}")

# زمان‌بندی اجرای روزانه
if __name__ == "__main__":
    # زمان‌بندی برای اجرا هر روز ساعت 19 به وقت تهران
    schedule.every().day.at("19:00").do(run_daily_task)

    print("برنامه زمان‌بندی شده برای اجرا هر روز ساعت 19 به وقت تهران.")
    while True:
        schedule.run_pending()
        time.sleep(60)  # بررسی هر 60 ثانیه
