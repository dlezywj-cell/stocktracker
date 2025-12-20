import akshare as ak
import pandas as pd
import json
import datetime
import time
import random  # 引入 random 库
import os

def fetch_and_save_data():
    print("Step 1: 获取申万(Shenwan)二级行业列表...")
    
    try:
        sw_boards = ak.sw_index_second_info()
        print(f"SUCCESS: 获取到 {len(sw_boards)} 个申万二级行业")
    except Exception as e:
        print(f"ERROR: 获取申万行业列表失败。")
        raise e

    # 设定时间范围 (最近2年)
    start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime("%Y%m%d")
    end_date = datetime.datetime.now().strftime("%Y%m%d")

    data_store = {}
    
    print("Step 2: 循环获取行业历史数据 (速度较慢，请耐心等待)...")
    
    max_retries = 3
    total = len(sw_boards)
    
    for i, row in sw_boards.iterrows():
        code = str(row['行业代码']).split('.')[0]
        name = row['行业名称']
        
        print(f"[{i+1}/{total}] 获取 {name} ({code})...", end="", flush=True)
        
        # === 唯一修改的地方 ===
        # 原来是 time.sleep(0.5)，太快容易被封。
        # 改成 1.5 秒，虽然总耗时会多几分钟，但每天只跑一次，稳最重要。
        time.sleep(1.5) 
        
        fetched = False
        for attempt in range(max_retries):
            try:
                df = ak.index_hist_sw(symbol=code, period="day")
                
                if df is None or df.empty:
                    raise ValueError("Empty Data")

                if '日期' in df.columns:
                    df['日期'] = pd.to_datetime(df['日期'])
                    df.set_index('日期', inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)
                
                df.sort_index(inplace=True)
                
                mask = (df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))
                df_subset = df.loc[mask]
                
                if df_subset.empty:
                    print(f" 无区间数据 - 跳过")
                else:
                    data_store[name] = df_subset['收盘']
                    print(f" 成功 ({len(df_subset)}条)")
                
                fetched = True
                break 
                
            except Exception as e:
                if attempt < max_retries - 1:
                    # 失败重试时多睡一会
                    time.sleep(3)
                else:
                    print(f" 失败") 

    print(f"\nStep 3: 数据清洗与对齐...")

    if not data_store:
        raise ValueError("严重错误：没有获取到任何申万数据！")

    df_all = pd.DataFrame(data_store)
    df_all.sort_index(inplace=True)
    df_all.fillna(method='ffill', inplace=True)
    df_all.fillna(0, inplace=True)

    common_dates = df_all.index.strftime('%Y-%m-%d').tolist()
    
    final_data_map = {}
    for col in df_all.columns:
        final_data_map[col] = df_all[col].round(4).tolist()

    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Shenwan Level 2 (AkShare)",
        "dates": common_dates,
        "data": final_data_map
    }

    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"SUCCESS: industry_data.json 保存成功。包含 {len(common_dates)} 个交易日，{len(final_data_map)} 个行业。")

if __name__ == "__main__":
    fetch_and_save_data()
