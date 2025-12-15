import pandas as pd
import pytz

# === 1. 读取 CSV ===
# CSV 格式：
# date,signal
# 2010-01-06,-2.12
# 2010-01-07,-1.22
csv_path = "Singular Square\summary\signals.csv" #替换文件路径
df = pd.read_csv(csv_path)

# === 2. 转换为纽约时区并转为毫秒 ===
ny_tz = pytz.timezone("America/New_York")
df['timestamp_ms'] = pd.to_datetime(df['date']).dt.tz_localize(ny_tz).astype('int64') // 10**6

# === 3. 生成 Pine 数组 ===
dates_list = ",".join(str(int(x)) for x in df['timestamp_ms'])
vals_list = ",".join(str(round(x, 2)) for x in df['signal'])

# === 4. 拼接成 Pine Script 函数 ===
pine_code = f"""f_load_dates() =>
    array.clear(dates)
    array.concat(dates, array.from({dates_list}))

f_load_vals() =>
    array.clear(vals)
    array.concat(vals, array.from({vals_list}))
"""

# === 5. 保存或打印 ===
with open("Singular Square\summary\pine_signal_arrays.txt", "w", encoding="utf-8") as f: #替换输出路径
    f.write(pine_code)

print("✅ 已生成 TradingView 用纽约时区毫秒时间戳格式：pine_signal_arrays.txt")