import akshare as ak
import pandas as pd
import json
import datetime
import time
import random
import os

def fetch_and_save_data():
    print("Step 1: 获取申万(Shenwan)二级行业列表...")
    
    try:
        # 接口: 获取申万二级行业列表
        # 列名通常包含: 行业代码, 行业名称
        sw_boards = ak.sw_index_second_info()
        print(f"SUCCESS: 获取到 {len(sw_boards)} 个申万二级行业")
    except Exception as e:
        print(f"ERROR: 获取申万行业列表失败。原因: {e}")
        # 如果列表都获取不到，后续无法进行，直接抛出异常终止
        raise e

    # 设定时间范围 (最近2年，既保证数据足够，又不会导致文件过大)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730)
    
    # 转换为字符串格式用于比较
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    data_store = {}
    
    print("Step 2: 循环获取行业历史数据 (稳定模式，速度较慢)...")
    
    max_retries = 3
    total = len(sw_boards)
    
    # 遍历每一行
    for i, row in sw_boards.iterrows():
        # 提取代码和名称
        # 代码处理: 有些接口返回 "801010.SI"，我们需要 "801010"
        code = str(row['行业代码']).split('.')[0] 
        name = row['行业名称']
        
        print(f"[{i+1}/{total}] 获取 {name} ({code})...", end="", flush=True)
        
        # === 核心安全机制: 随机休眠 1.0 ~ 1.5 秒 ===
        # 既然是每天收盘后跑一次，慢一点无所谓，稳最重要
        time.sleep(random.uniform(1.0, 1.5))
        
        fetched = False
        for attempt in range(max_retries):
            try:
                # 接口: 获取指数历史行情
                df = ak.index_hist_sw(symbol=code, period="day")
                
                if df is None or df.empty:
                    # 某些冷门或新上市行业可能没数据
                    raise ValueError("返回为空")

                # 数据清洗：统一索引为日期格式
                if '日期' in df.columns:
                    df['日期'] = pd.to_datetime(df['日期'])
                    df.set_index('日期', inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)
                
                df.sort_index(inplace=True)
                
                # 截取时间段
                mask = (df.index >= pd.to_datetime(start_str)) & (df.index <= pd.to_datetime(end_str))
                df_subset = df.loc[mask]
                
                if df_subset.empty:
                    print(f" 无区间内数据 - 跳过")
                else:
                    # 提取收盘价
                    data_store[name] = df_subset['收盘']
                    print(f" 成功 ({len(df_subset)}条)")
                
                fetched = True
                break # 成功则跳出重试循环
                
            except Exception as e:
                if attempt < max_retries - 1:
                    # 失败后多等待一会儿再重试
                    time.sleep(2)
                else:
                    print(f" 失败") # 最终失败，不打印详细堆栈以免刷屏

    print(f"\nStep 3: 数据对齐与保存...")

    if not data_store:
        raise ValueError("严重错误：没有获取到任何申万数据！请检查AkShare接口是否可用。")

    # --- 数据对齐逻辑 (关键) ---
    # 1. 转为 DataFrame，Pandas 会自动按日期索引对齐
    df_all = pd.DataFrame(data_store)
    df_all.sort_index(inplace=True)
    
    # 2. 填充缺失值
    # ffill: 用前一天的收盘价填充停牌日，保证曲线连续
    df_all.fillna(method='ffill', inplace=True)
    # fillna(0): 处理上市前的空白期
    df_all.fillna(0, inplace=True)

    # 3. 提取公共日期列表 (字符串格式)
    common_dates = df_all.index.strftime('%Y-%m-%d').tolist()
    
    # 4. 格式化数据 (保留4位小数，减少体积)
    final_data_map = {}
    for col in df_all.columns:
        final_data_map[col] = df_all[col].round(4).tolist()

    # 5. 构建最终 JSON
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Shenwan Level 2 (AkShare)",
        "dates": common_dates,
        "data": final_data_map
    }

    # 6. 写入文件
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"SUCCESS: industry_data.json 保存成功。")
    print(f"数据统计: {len(common_dates)} 个交易日, {len(final_data_map)} 个行业。")

if __name__ == "__main__":
    fetch_and_save_data()
