import csv
import json

input_file = 'داده های تاریخی دلار - Sheet1.csv'
output_file = 'usd_daily_prices.json'

result = {}

with open(input_file, encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        date_shamsi = row['date shamsi'].strip()
        close_price = row['close'].replace(',', '').strip()
        if date_shamsi and close_price:
            try:
                result[date_shamsi] = float(close_price)
            except ValueError:
                continue

with open(output_file, 'w', encoding='utf-8') as jsonfile:
    json.dump(result, jsonfile, ensure_ascii=False, indent=2)

print(f'فایل {output_file} با موفقیت ساخته شد.')
