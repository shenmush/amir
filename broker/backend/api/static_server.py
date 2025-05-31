from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI()

# مسیر مطلق پوشه frontend
frontend_path = r"C:\Users\Administrator\Desktop\broker\frontend"

# سرو کردن فایل‌های استاتیک
app.mount("/", StaticFiles(directory=frontend_path, html=True), name="static") 