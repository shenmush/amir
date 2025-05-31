from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import logging
import numpy as np
from typing import Optional  # وارد کردن Optional
from datetime import datetime
# 🔹 تنظیمات لاگ
logging.basicConfig(level=logging.INFO)

# 🔹 ایجاد اپلیکیشن FastAPI
app = FastAPI()

# 🔹 افزودن CORS Middleware (برای ارتباط با فرانت‌اند)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

# 🔹 تنظیمات دیتابیس
DB_SERVER = "localhost"
DB_NAME = "ime"
DATABASE_URL = f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"

# 🔹 ایجاد Engine برای اتصال به دیتابیس
engine = create_engine(DATABASE_URL)

@app.get("/premium/data")
async def get_premium_data(
    product_name: str = None,
    producer: str = None,
    contract_type: str = None,
    unique: bool = False,
    search: str = None,
    start_date: str = None,
    end_date: str = None
):
    try:
        # 🔹 **جستجو در `product_name`، `producer` و `contract_type`**
        if search:
            query = text("""
                SELECT DISTINCT product_name, producer, contract_type
                FROM premium
                WHERE product_name LIKE :search 
                   OR producer LIKE :search 
                   OR contract_type LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        if unique:
            unique_data = {}

            # 🔹 اگر هیچ فیلتر انتخاب نشده باشد، همه مقادیر یکتا را بگیر
            if not product_name and not producer and not contract_type:
                query = text("SELECT DISTINCT product_name FROM premium")
                df = pd.read_sql(query, engine)
                unique_data["products"] = df["product_name"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT producer FROM premium")
                df = pd.read_sql(query, engine)
                unique_data["producers"] = df["producer"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT contract_type FROM premium")
                df = pd.read_sql(query, engine)
                unique_data["contract_types"] = df["contract_type"].dropna().tolist() if not df.empty else []

                return unique_data

            # 🔹 **فیلترهای وابسته: اگر `product_name` مقدار داشته باشد، `producer` و `contract_type` را بگیر**
            filters = []
            params = {}

            if product_name:
                filters.append("product_name LIKE :product_name")
                params["product_name"] = f"%{product_name}%"
            if producer:
                filters.append("producer LIKE :producer")
                params["producer"] = f"%{producer}%"
            if contract_type:
                filters.append("contract_type LIKE :contract_type")
                params["contract_type"] = f"%{contract_type}%"

            query = f"SELECT DISTINCT producer, contract_type FROM premium WHERE {' AND '.join(filters)}"
            df = pd.read_sql(text(query), engine, params=params)

            unique_data["producers"] = df["producer"].dropna().tolist() if "producer" in df.columns and not df.empty else []
            unique_data["contract_types"] = df["contract_type"].dropna().tolist() if "contract_type" in df.columns and not df.empty else []

            return unique_data

        # 🔹 **اگر unique=False بود، داده‌های فیلتر شده را برگردان**
        query_str = """
            SELECT * FROM premium
            WHERE (:product_name IS NULL OR product_name LIKE :product_name)
            AND (:producer IS NULL OR producer LIKE :producer)
            AND (:contract_type IS NULL OR contract_type LIKE :contract_type)
        """
        if start_date and end_date:
            query_str += " AND (transaction_date BETWEEN :start_date AND :end_date)"
        query = text(query_str)
        params = {
            "product_name": f"%{product_name}%" if product_name else None,
            "producer": f"%{producer}%" if producer else None,
            "contract_type": f"%{contract_type}%" if contract_type else None,
        }
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
        df = pd.read_sql(query, engine, params=params)
        df = df.replace({np.nan: None})  # مدیریت مقدار `NaN`
        return {"filtered_data": df.to_dict(orient="records")} if not df.empty else {"filtered_data": []}

    except Exception as e:
        logging.error(f"خطا در دریافت داده‌های پرمیوم: {str(e)}")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده‌های پرمیوم.")

@app.get("/markets/data")
async def get_kala_data(
    market: str = None,
    product_name: str = None,
    producer: str = None,
    contract_type: str = None,
    unique: bool = False,
    search: str = None,
    start_date: str = None,
    end_date: str = None
):
    try:
        # ✅ **حالت جستجوی هوشمند**
        if search:
            query = text("""
                SELECT DISTINCT market, product_name, producer
                FROM kala
                WHERE market LIKE :search 
                   OR product_name LIKE :search 
                   OR producer LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        if unique:
            unique_data = {}

            # ✅ **ایجاد شرط‌های فیلتر به‌صورت پویا**
            filters = ["1=1"]
            params = {}

            if market:
                filters.append("market LIKE :market")
                params["market"] = f"%{market}%"
            if product_name:
                filters.append("product_name LIKE :product_name")
                params["product_name"] = f"%{product_name}%"
            if producer:
                filters.append("producer LIKE :producer")
                params["producer"] = f"%{producer}%"
            if contract_type:
                filters.append("contract_type = :contract_type")  # 👈 بدون LIKE
                params["contract_type"] = contract_type

            filter_clause = " AND ".join(filters)

            # ✅ **دریافت همه مقادیر یکتا بر اساس فیلترهای انتخاب شده**
            for column in ["market", "product_name", "producer", "contract_type"]:
                query = text(f"SELECT DISTINCT {column} FROM kala WHERE {filter_clause}")
                df = pd.read_sql(query, engine, params=params)
                unique_data[column + "s"] = df[column].dropna().tolist() if not df.empty else []

            return unique_data

        # ✅ **دریافت داده‌های فیلتر شده در صورتی که `unique=False` باشد**
        query_str = """
            SELECT * FROM kala
            WHERE (:market IS NULL OR market LIKE :market)
            AND (:product_name IS NULL OR product_name LIKE :product_name)
            AND (:producer IS NULL OR producer LIKE :producer)
            AND (:contract_type IS NULL OR contract_type = :contract_type)
        """
        if start_date and end_date:
            query_str += " AND (transaction_date BETWEEN :start_date AND :end_date)"
        query = text(query_str)
        params = {
            "market": f"%{market}%" if market else None,
            "product_name": f"%{product_name}%" if product_name else None,
            "producer": f"%{producer}%" if producer else None,
            "contract_type": contract_type if contract_type else None
        }
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
        df = pd.read_sql(query, engine, params=params)
        df = df.replace({np.nan: None})  # مدیریت مقدار `NaN`
        if df.empty:
            logging.warning("⛔ هیچ داده‌ای پیدا نشد!")
            return {"filtered_data": []}
        return {"filtered_data": df.to_dict(orient="records")}

    except Exception as e:
        logging.error(f"❌ خطا در دریافت داده‌های بازار کالا: {str(e)}")
        raise HTTPException(status_code=500, detail=f"خطا در دریافت داده‌های بازار کالا: {str(e)}")

@app.get("/export/data")
async def get_export_data(
    market: str = None,
    product_name: str = None,
    producer: str = None,
    contract_type: str = None,
    unique: bool = False,
    search: str = None,
    start_date: str = None,
    end_date: str = None
):
    try:
        # 🔹 **جستجو در `market`، `product_name`، `producer` و `contract_type`**
        if search:
            query = text("""
                SELECT DISTINCT market, product_name, producer, contract_type 
                FROM export
                WHERE market LIKE :search 
                   OR product_name LIKE :search 
                   OR producer LIKE :search 
                   OR contract_type LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        if unique:
            unique_data = {}

            # 🔹 **مرحله 1: دریافت کل مقادیر یکتا در صورت نبود فیلتر**
            if not market and not product_name and not producer and not contract_type:
                query = text("SELECT DISTINCT market FROM export")
                df = pd.read_sql(query, engine)
                unique_data["markets"] = df["market"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT product_name FROM export")
                df = pd.read_sql(query, engine)
                unique_data["products"] = df["product_name"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT producer FROM export")
                df = pd.read_sql(query, engine)
                unique_data["producers"] = df["producer"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT contract_type FROM export")
                df = pd.read_sql(query, engine)
                unique_data["contract_types"] = df["contract_type"].dropna().tolist() if not df.empty else []

                return unique_data

            # 🔹 **مرحله 2: فیلتر کردن مقادیر یکتا مرحله به مرحله**
            filters = []
            params = {}

            if market:
                filters.append("market LIKE :market")
                params["market"] = f"%{market}%"
            if product_name:
                filters.append("product_name LIKE :product_name")
                params["product_name"] = f"%{product_name}%"
            if producer:
                filters.append("producer LIKE :producer")
                params["producer"] = f"%{producer}%"
            if contract_type:
                filters.append("contract_type LIKE :contract_type")
                params["contract_type"] = f"%{contract_type}%"

            query = f"SELECT DISTINCT product_name, producer, contract_type FROM export WHERE {' AND '.join(filters)}"
            df = pd.read_sql(text(query), engine, params=params)

            unique_data["products"] = df["product_name"].dropna().tolist() if "product_name" in df.columns and not df.empty else []
            unique_data["producers"] = df["producer"].dropna().tolist() if "producer" in df.columns and not df.empty else []
            unique_data["contract_types"] = df["contract_type"].dropna().tolist() if "contract_type" in df.columns and not df.empty else []

            return unique_data

        # 🔹 **اگر unique=False بود، داده‌های فیلتر شده را برگردان**
        query_str = """
            SELECT * FROM export
            WHERE (:market IS NULL OR market LIKE :market)
            AND (:product_name IS NULL OR product_name LIKE :product_name)
            AND (:producer IS NULL OR producer LIKE :producer)
            AND (:contract_type IS NULL OR contract_type LIKE :contract_type)
        """
        if start_date and end_date:
            query_str += " AND (transaction_date BETWEEN :start_date AND :end_date)"
        query = text(query_str)
        params = {
            "market": f"%{market}%" if market else None,
            "product_name": f"%{product_name}%" if product_name else None,
            "producer": f"%{producer}%" if producer else None,
            "contract_type": f"%{contract_type}%" if contract_type else None
        }
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
        df = pd.read_sql(query, engine, params=params)
        df = df.replace({np.nan: None})  # مدیریت مقدار `NaN`
        return {"filtered_data": df.to_dict(orient="records")} if not df.empty else {"filtered_data": []}

    except Exception as e:
        logging.error(f"خطا در دریافت داده‌های صادراتی: {str(e)}")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده‌های صادراتی.")

@app.get("/future/data")
async def get_future_data(
    description: str = None,
    contract: str = None,
    unique: bool = False,
    search: str = None,
    start_date: str = None,
    end_date: str = None
):
    try:
        # 🔹 **جستجو در `description` و `contract`**
        if search:
            query = text("""
                SELECT DISTINCT description, contract 
                FROM future
                WHERE description LIKE :search 
                   OR contract LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        if unique:
            unique_data = {}

            # 🔹 اگر هیچ فیلتر انتخاب نشده باشد، همه مقادیر یکتا را بگیر
            if not description and not contract:
                query = text("SELECT DISTINCT description FROM future")
                df = pd.read_sql(query, engine)
                unique_data["descriptions"] = df["description"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT contract FROM future")
                df = pd.read_sql(query, engine)
                unique_data["contracts"] = df["contract"].dropna().tolist() if not df.empty else []

                return unique_data

            # 🔹 اگر `description` مقدار داشته باشد، مقدار یکتا بر اساس آن بگیر
            if description and not contract:
                query = text("SELECT DISTINCT contract FROM future WHERE description LIKE :description")
                df = pd.read_sql(query, engine, params={"description": f"%{description}%"})
                unique_data["contracts"] = df["contract"].dropna().tolist() if not df.empty else []
                return unique_data

            # 🔹 اگر `contract` مقدار داشته باشد، مقدار یکتا بر اساس آن بگیر
            if contract and not description:
                query = text("SELECT DISTINCT description FROM future WHERE contract LIKE :contract")
                df = pd.read_sql(query, engine, params={"contract": f"%{contract}%"})
                unique_data["descriptions"] = df["description"].dropna().tolist() if not df.empty else []
                return unique_data

        # 🔹 **اگر unique=False بود، داده‌های فیلتر شده را برگردان**
        query_str = """
            SELECT * FROM future
            WHERE (:description IS NULL OR description LIKE :description)
            AND (:contract IS NULL OR contract LIKE :contract)
        """
        if start_date and end_date:
            query_str += " AND (transaction_date BETWEEN :start_date AND :end_date)"
        query = text(query_str)
        params = {
            "description": f"%{description}%" if description else None,
            "contract": f"%{contract}%" if contract else None
        }
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
        df = pd.read_sql(query, engine, params=params)
        df = df.replace({np.nan: None})  # مدیریت مقدار `NaN`
        return {"filtered_data": df.to_dict(orient="records")} if not df.empty else {"filtered_data": []}

    except Exception as e:
        logging.error(f"خطا در دریافت داده‌های بازار آتی: {str(e)}")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده‌های بازار آتی.")

@app.get("/financial/data")
async def get_financial_data(
    description: str = None,
    unique: bool = False,
    search: str = None,
    start_date: str = None,
    end_date: str = None
):
    try:
        # 🔹 **جستجو در فیلد `description`**
        if search:
            query = text("""
                SELECT DISTINCT description 
                FROM financial
                WHERE description LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df["description"].dropna().tolist()} if not df.empty else {"search_results": []}

        if unique:
            # 🔹 اگر هیچ فیلتر انتخاب نشده باشد، تمام مقادیر یکتا را بگیر
            query = text("SELECT DISTINCT description FROM financial")
            df = pd.read_sql(query, engine)
            return {"descriptions": df["description"].dropna().tolist()} if not df.empty else {"descriptions": []}

        # 🔹 **اگر unique=False بود، داده‌های فیلتر شده را برگردان**
        if description or (start_date and end_date):
            query_str = """
                SELECT * FROM financial
                WHERE (:description IS NULL OR description LIKE :description)
            """
            if start_date and end_date:
                query_str += " AND (transaction_date BETWEEN :start_date AND :end_date)"
            query = text(query_str)
            params = {
                "description": f"%{description}%" if description else None
            }
            if start_date and end_date:
                params["start_date"] = start_date
                params["end_date"] = end_date
            df = pd.read_sql(query, engine, params=params)
            df = df.replace({np.nan: None})  # مدیریت مقدار `NaN`
            return {"filtered_data": df.to_dict(orient="records")} if not df.empty else {"filtered_data": []}

        # 🔹 اگر هیچ مقدار مشخص نشده باشد، لیست کامل برگردان
        query_str = "SELECT * FROM financial"
        if start_date and end_date:
            query_str += " WHERE (transaction_date BETWEEN :start_date AND :end_date)"
            query = text(query_str)
            params = {
                "start_date": start_date,
                "end_date": end_date
            }
            df = pd.read_sql(query, engine, params=params)
        else:
            query = text(query_str)
            df = pd.read_sql(query, engine)
        df = df.replace({np.nan: None})
        return {"all_data": df.to_dict(orient="records")} if not df.empty else {"all_data": []}

    except Exception as e:
        logging.error(f"خطا در دریافت داده‌های بازار مالی: {str(e)}")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده‌های بازار مالی.")

@app.get("/estate/data")
async def get_estate_data(
    product_name: str = None,
    delivery_location: str = None,
    supplier: str = None,
    unique: bool = False,
    search: str = None,
    start_date: str = None,
    end_date: str = None
):
    try:
        # 🔹 **جستجو در همه فیلدها**
        if search:
            query = text("""
                SELECT DISTINCT Product_name, delivery_location, supplier 
                FROM estate
                WHERE Product_name LIKE :search 
                   OR delivery_location LIKE :search 
                   OR supplier LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        if unique:
            unique_data = {}

            # 🔹 اگر هیچ فیلتر انتخاب نشده باشد، همه مقادیر یکتا را بگیر
            if not product_name and not delivery_location and not supplier:
                query = text("SELECT DISTINCT Product_name FROM estate")
                df = pd.read_sql(query, engine)
                unique_data["products"] = df["Product_name"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT delivery_location FROM estate")
                df = pd.read_sql(query, engine)
                unique_data["locations"] = df["delivery_location"].dropna().tolist() if not df.empty else []

                query = text("SELECT DISTINCT supplier FROM estate")
                df = pd.read_sql(query, engine)
                unique_data["suppliers"] = df["supplier"].dropna().tolist() if not df.empty else []

                return unique_data

            # 🔹 **اگر فقط یک مقدار داده شده باشد، مقدار یکتا را بر اساس آن بگیر**
            if product_name and not delivery_location and not supplier:
                query = text("SELECT DISTINCT delivery_location, supplier FROM estate WHERE Product_name LIKE :product_name")
                df = pd.read_sql(query, engine, params={"product_name": f"%{product_name}%"})
                unique_data["locations"] = df["delivery_location"].dropna().tolist() if not df.empty else []
                unique_data["suppliers"] = df["supplier"].dropna().tolist() if not df.empty else []
                return unique_data

            if delivery_location and not product_name and not supplier:
                query = text("SELECT DISTINCT Product_name, supplier FROM estate WHERE delivery_location LIKE :delivery_location")
                df = pd.read_sql(query, engine, params={"delivery_location": f"%{delivery_location}%"})
                unique_data["products"] = df["Product_name"].dropna().tolist() if not df.empty else []
                unique_data["suppliers"] = df["supplier"].dropna().tolist() if not df.empty else []
                return unique_data

            if supplier and not product_name and not delivery_location:
                query = text("SELECT DISTINCT Product_name, delivery_location FROM estate WHERE supplier LIKE :supplier")
                df = pd.read_sql(query, engine, params={"supplier": f"%{supplier}%"})
                unique_data["products"] = df["Product_name"].dropna().tolist() if not df.empty else []
                unique_data["locations"] = df["delivery_location"].dropna().tolist() if not df.empty else []
                return unique_data

            # 🔹 **اگر دو مقدار داده شد، ابتدا مقدار اول را فیلتر کن، سپس مقدار یکتا را برای مقدار دوم بگیر**
            filters = []
            params = {}

            if product_name:
                filters.append("Product_name LIKE :product_name")
                params["product_name"] = f"%{product_name}%"
            if delivery_location:
                filters.append("delivery_location LIKE :delivery_location")
                params["delivery_location"] = f"%{delivery_location}%"
            if supplier:
                filters.append("supplier LIKE :supplier")
                params["supplier"] = f"%{supplier}%"

            query = "SELECT * FROM estate WHERE " + " AND ".join(filters)
            df = pd.read_sql(text(query), engine, params=params)

            if df.empty:
                return {"filtered_results": []}

            unique_data = {}

            if product_name and delivery_location:
                unique_data["suppliers"] = df["supplier"].dropna().unique().tolist()

            if product_name and supplier:
                unique_data["locations"] = df["delivery_location"].dropna().unique().tolist()

            if delivery_location and supplier:
                unique_data["products"] = df["Product_name"].dropna().unique().tolist()

            return unique_data

        # 🔹 **اگر unique=False بود، داده‌های فیلتر شده را برگردان**
        query_str = """
            SELECT * FROM estate
            WHERE (:product_name IS NULL OR Product_name LIKE :product_name)
            AND (:delivery_location IS NULL OR delivery_location LIKE :delivery_location)
            AND (:supplier IS NULL OR supplier LIKE :supplier)
        """
        if start_date and end_date:
            query_str += " AND (transaction_date BETWEEN :start_date AND :end_date)"
        query = text(query_str)
        params = {
            "product_name": f"%{product_name}%" if product_name else None,
            "delivery_location": f"%{delivery_location}%" if delivery_location else None,
            "supplier": f"%{supplier}%" if supplier else None
        }
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
        df = pd.read_sql(query, engine, params=params)
        df = df.replace({np.nan: None})  # مدیریت مقدار `NaN`
        return {"filtered_data": df.to_dict(orient="records")} if not df.empty else {"filtered_data": []}

    except Exception as e:
        logging.error(f"خطا در دریافت داده‌های بازار املاک: {str(e)}")
        raise HTTPException(status_code=500, detail="خطا در دریافت داده‌های بازار املاک.")

@app.get("/arzekala/data")
async def get_arze_kala_data(
    start_date: str = None,  # تاریخ شروع
    end_date: str = None,    # تاریخ پایان
    bArzehRadifNamadKala: str = None,
    xTolidKonandehSharh: str = None,
    Talar: str = None,
    cBrokerSpcName: str = None,
    unique: bool = False,
    search: str = None
):
    try:
        # ✅ **حالت جستجوی هوشمند**
        if search:
            query = text("""
                SELECT DISTINCT transaction_date, bArzehRadifNamadKala, xTolidKonandehSharh, Talar, cBrokerSpcName
                FROM arze_kala
                WHERE transaction_date LIKE :search 
                   OR bArzehRadifNamadKala LIKE :search 
                   OR xTolidKonandehSharh LIKE :search
                   OR Talar LIKE :search
                   OR cBrokerSpcName LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        # گرفتن اولین و آخرین تاریخ از دیتابیس
        date_range_query = text("SELECT MIN(transaction_date) AS min_date, MAX(transaction_date) AS max_date FROM arze_kala")
        date_range = pd.read_sql(date_range_query, engine).iloc[0]
        min_date = date_range["min_date"]
        max_date = date_range["max_date"]

        if unique:
            unique_data = {}
            filters = ["1=1"]
            params = {}

            # فیلتر تاریخ شروع و پایان
            if start_date and end_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = start_date
                params["end_date"] = end_date
            elif start_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = start_date
                params["end_date"] = max_date  # تا آخرین تاریخ
            elif end_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = min_date  # از اولین تاریخ
                params["end_date"] = end_date
            # اگه هیچ‌کدوم نباشه، همه تاریخ‌ها (min_date تا max_date) رو می‌گیره

            if bArzehRadifNamadKala:
                filters.append("bArzehRadifNamadKala LIKE :bArzehRadifNamadKala")
                params["bArzehRadifNamadKala"] = f"%{bArzehRadifNamadKala}%"
            if xTolidKonandehSharh:
                filters.append("xTolidKonandehSharh LIKE :xTolidKonandehSharh")
                params["xTolidKonandehSharh"] = f"%{xTolidKonandehSharh}%"
            if Talar:
                filters.append("Talar LIKE :Talar")
                params["Talar"] = f"%{Talar}%"
            if cBrokerSpcName:
                filters.append("cBrokerSpcName LIKE :cBrokerSpcName")
                params["cBrokerSpcName"] = f"%{cBrokerSpcName}%"

            filter_clause = " AND ".join(filters)
            for column in ["bArzehRadifNamadKala", "xTolidKonandehSharh", "Talar", "cBrokerSpcName"]:
                query = text(f"SELECT DISTINCT {column} FROM arze_kala WHERE {filter_clause}")
                df = pd.read_sql(query, engine, params=params)
                unique_data[column + "s"] = df[column].dropna().tolist() if not df.empty else []

            return unique_data

        # ✅ **دریافت داده‌های فیلتر شده در صورتی که `unique=False` باشد**
        params = {
            "bArzehRadifNamadKala": f"%{bArzehRadifNamadKala}%" if bArzehRadifNamadKala else None,
            "xTolidKonandehSharh": f"%{xTolidKonandehSharh}%" if xTolidKonandehSharh else None,
            "Talar": f"%{Talar}%" if Talar else None,
            "cBrokerSpcName": f"%{cBrokerSpcName}%" if cBrokerSpcName else None
        }

        if start_date and end_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = start_date
            params["end_date"] = end_date
        elif start_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = start_date
            params["end_date"] = max_date  # تا آخرین تاریخ
        elif end_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = min_date  # از اولین تاریخ
            params["end_date"] = end_date
        else:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = min_date  # از اولین تا آخرین
            params["end_date"] = max_date

        query = text(f"""
            SELECT * FROM arze_kala
            WHERE {date_filter}
            AND (:bArzehRadifNamadKala IS NULL OR bArzehRadifNamadKala LIKE :bArzehRadifNamadKala)
            AND (:xTolidKonandehSharh IS NULL OR xTolidKonandehSharh LIKE :xTolidKonandehSharh)
            AND (:Talar IS NULL OR Talar LIKE :Talar)
            AND (:cBrokerSpcName IS NULL OR cBrokerSpcName LIKE :cBrokerSpcName)
        """)
        df = pd.read_sql(query, engine, params=params)

        df = df.replace({np.nan: None})
        if df.empty:
            logging.warning("⛔ هیچ داده‌ای پیدا نشد!")
            return {"filtered_data": []}

        return {"filtered_data": df.to_dict(orient="records")}

    except Exception as e:
        logging.error(f"❌ خطا در دریافت داده‌های عرضه کالا: {str(e)}")
        raise HTTPException(status_code=500, detail=f"خطا در دریافت داده‌های عرضه کالا: {str(e)}")

@app.get("/arzeexport/data")
async def get_arze_export_data(
    start_date: str = None,  # تاریخ شروع
    end_date: str = None,    # تاریخ پایان
    bArzehRadifNamadKala: str = None,
    xTolidKonandehSharh: str = None,
    Talar: str = None,
    cBrokerSpcName: str = None,
    unique: bool = False,
    search: str = None
):
    try:
        # ✅ **حالت جستجوی هوشمند**
        if search:
            query = text("""
                SELECT DISTINCT transaction_date, bArzehRadifNamadKala, xTolidKonandehSharh, Talar, cBrokerSpcName
                FROM arze_export
                WHERE transaction_date LIKE :search 
                   OR bArzehRadifNamadKala LIKE :search 
                   OR xTolidKonandehSharh LIKE :search
                   OR Talar LIKE :search
                   OR cBrokerSpcName LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        # گرفتن اولین و آخرین تاریخ از دیتابیس
        date_range_query = text("SELECT MIN(transaction_date) AS min_date, MAX(transaction_date) AS max_date FROM arze_export")
        date_range = pd.read_sql(date_range_query, engine).iloc[0]
        min_date = date_range["min_date"]
        max_date = date_range["max_date"]

        if unique:
            unique_data = {}
            filters = ["1=1"]
            params = {}

            # فیلتر تاریخ شروع و پایان (مثل arzekala)
            if start_date and end_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = start_date
                params["end_date"] = end_date
            elif start_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = start_date
                params["end_date"] = max_date  # تا آخرین تاریخ
            elif end_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = min_date  # از اولین تاریخ
                params["end_date"] = end_date
            # اگه هیچ‌کدوم نباشه، همه تاریخ‌ها (min_date تا max_date) رو می‌گیره

            if bArzehRadifNamadKala:
                filters.append("bArzehRadifNamadKala LIKE :bArzehRadifNamadKala")
                params["bArzehRadifNamadKala"] = f"%{bArzehRadifNamadKala}%"
            if xTolidKonandehSharh:
                filters.append("xTolidKonandehSharh LIKE :xTolidKonandehSharh")
                params["xTolidKonandehSharh"] = f"%{xTolidKonandehSharh}%"
            if Talar:
                filters.append("Talar LIKE :Talar")
                params["Talar"] = f"%{Talar}%"
            if cBrokerSpcName:
                filters.append("cBrokerSpcName LIKE :cBrokerSpcName")
                params["cBrokerSpcName"] = f"%{cBrokerSpcName}%"

            filter_clause = " AND ".join(filters)
            for column in ["bArzehRadifNamadKala", "xTolidKonandehSharh", "Talar", "cBrokerSpcName"]:
                query = text(f"SELECT DISTINCT {column} FROM arze_export WHERE {filter_clause}")
                df = pd.read_sql(query, engine, params=params)
                unique_data[column + "s"] = df[column].dropna().tolist() if not df.empty else []

            return unique_data

        # ✅ **دریافت داده‌های فیلتر شده در صورتی که `unique=False` باشد**
        params = {
            "bArzehRadifNamadKala": f"%{bArzehRadifNamadKala}%" if bArzehRadifNamadKala else None,
            "xTolidKonandehSharh": f"%{xTolidKonandehSharh}%" if xTolidKonandehSharh else None,
            "Talar": f"%{Talar}%" if Talar else None,
            "cBrokerSpcName": f"%{cBrokerSpcName}%" if cBrokerSpcName else None
        }

        if start_date and end_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = start_date
            params["end_date"] = end_date
        elif start_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = start_date
            params["end_date"] = max_date  # تا آخرین تاریخ
        elif end_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = min_date  # از اولین تاریخ
            params["end_date"] = end_date
        else:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = min_date  # از اولین تا آخرین
            params["end_date"] = max_date

        query = text(f"""
            SELECT * FROM arze_export
            WHERE {date_filter}
            AND (:bArzehRadifNamadKala IS NULL OR bArzehRadifNamadKala LIKE :bArzehRadifNamadKala)
            AND (:xTolidKonandehSharh IS NULL OR xTolidKonandehSharh LIKE :xTolidKonandehSharh)
            AND (:Talar IS NULL OR Talar LIKE :Talar)
            AND (:cBrokerSpcName IS NULL OR cBrokerSpcName LIKE :cBrokerSpcName)
        """)
        df = pd.read_sql(query, engine, params=params)

        df = df.replace({np.nan: None})
        if df.empty:
            logging.warning("⛔ هیچ داده‌ای پیدا نشد!")
            return {"filtered_data": []}

        return {"filtered_data": df.to_dict(orient="records")}

    except Exception as e:
        logging.error(f"❌ خطا در دریافت داده‌های عرضه صادراتی: {str(e)}")
        raise HTTPException(status_code=500, detail=f"خطا در دریافت داده‌های عرضه صادراتی: {str(e)}")

@app.get("/arzepremium/data")
async def get_arze_premium_data(
    start_date: str = None,  # تاریخ شروع
    end_date: str = None,    # تاریخ پایان
    bArzehRadifNamadKala: str = None,
    xTolidKonandehSharh: str = None,
    cBrokerSpcName: str = None,
    unique: bool = False,
    search: str = None
):
    try:
        # ✅ **حالت جستجوی هوشمند**
        if search:
            query = text("""
                SELECT DISTINCT transaction_date, bArzehRadifNamadKala, xTolidKonandehSharh, cBrokerSpcName
                FROM arze_premium
                WHERE transaction_date LIKE :search 
                   OR bArzehRadifNamadKala LIKE :search 
                   OR xTolidKonandehSharh LIKE :search
                   OR cBrokerSpcName LIKE :search
            """)
            df = pd.read_sql(query, engine, params={"search": f"%{search}%"})
            return {"search_results": df.to_dict(orient="records")} if not df.empty else {"search_results": []}

        # گرفتن اولین و آخرین تاریخ از دیتابیس
        date_range_query = text("SELECT MIN(transaction_date) AS min_date, MAX(transaction_date) AS max_date FROM arze_premium")
        date_range = pd.read_sql(date_range_query, engine).iloc[0]
        min_date = date_range["min_date"]
        max_date = date_range["max_date"]

        if unique:
            unique_data = {}
            filters = ["1=1"]
            params = {}

            # فیلتر تاریخ شروع و پایان (مثل arzekala)
            if start_date and end_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = start_date
                params["end_date"] = end_date
            elif start_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = start_date
                params["end_date"] = max_date  # تا آخرین تاریخ
            elif end_date:
                filters.append("transaction_date BETWEEN :start_date AND :end_date")
                params["start_date"] = min_date  # از اولین تاریخ
                params["end_date"] = end_date
            # اگه هیچ‌کدوم نباشه، همه تاریخ‌ها (min_date تا max_date) رو می‌گیره

            if bArzehRadifNamadKala:
                filters.append("bArzehRadifNamadKala LIKE :bArzehRadifNamadKala")
                params["bArzehRadifNamadKala"] = f"%{bArzehRadifNamadKala}%"
            if xTolidKonandehSharh:
                filters.append("xTolidKonandehSharh LIKE :xTolidKonandehSharh")
                params["xTolidKonandehSharh"] = f"%{xTolidKonandehSharh}%"
            if cBrokerSpcName:
                filters.append("cBrokerSpcName LIKE :cBrokerSpcName")
                params["cBrokerSpcName"] = f"%{cBrokerSpcName}%"

            filter_clause = " AND ".join(filters)
            for column in ["bArzehRadifNamadKala", "xTolidKonandehSharh", "cBrokerSpcName"]:
                query = text(f"SELECT DISTINCT {column} FROM arze_premium WHERE {filter_clause}")
                df = pd.read_sql(query, engine, params=params)
                unique_data[column + "s"] = df[column].dropna().tolist() if not df.empty else []

            return unique_data

        # ✅ **دریافت داده‌های فیلتر شده در صورتی که `unique=False` باشد**
        params = {
            "bArzehRadifNamadKala": f"%{bArzehRadifNamadKala}%" if bArzehRadifNamadKala else None,
            "xTolidKonandehSharh": f"%{xTolidKonandehSharh}%" if xTolidKonandehSharh else None,
            "cBrokerSpcName": f"%{cBrokerSpcName}%" if cBrokerSpcName else None
        }

        if start_date and end_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = start_date
            params["end_date"] = end_date
        elif start_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = start_date
            params["end_date"] = max_date  # تا آخرین تاریخ
        elif end_date:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = min_date  # از اولین تاریخ
            params["end_date"] = end_date
        else:
            date_filter = "transaction_date BETWEEN :start_date AND :end_date"
            params["start_date"] = min_date  # از اولین تا آخرین
            params["end_date"] = max_date

        query = text(f"""
            SELECT * FROM arze_premium
            WHERE {date_filter}
            AND (:bArzehRadifNamadKala IS NULL OR bArzehRadifNamadKala LIKE :bArzehRadifNamadKala)
            AND (:xTolidKonandehSharh IS NULL OR xTolidKonandehSharh LIKE :xTolidKonandehSharh)
            AND (:cBrokerSpcName IS NULL OR cBrokerSpcName LIKE :cBrokerSpcName)
        """)
        df = pd.read_sql(query, engine, params=params)

        df = df.replace({np.nan: None})
        if df.empty:
            logging.warning("⛔ هیچ داده‌ای پیدا نشد!")
            return {"filtered_data": []}

        return {"filtered_data": df.to_dict(orient="records")}

    except Exception as e:
        logging.error(f"❌ خطا در دریافت داده‌های عرضه پرمیوم: {str(e)}")
        raise HTTPException(status_code=500, detail=f"خطا در دریافت داده‌های عرضه پرمیوم: {str(e)}")

@app.get("/usd-prices")
async def get_usd_prices():
    try:
        query = text("SELECT date_shamsi, usd_price FROM usd_prices")
        df = pd.read_sql(query, engine)
        if df.empty:
            return {}
        # تبدیل به دیکشنری تاریخ شمسی → قیمت دلار
        return {row['date_shamsi']: row['usd_price'] for _, row in df.iterrows()}
    except Exception as e:
        logging.error(f"خطا در دریافت قیمت دلار: {str(e)}")
        raise HTTPException(status_code=500, detail="خطا در دریافت قیمت دلار.")