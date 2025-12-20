import akshare as ak
import pandas as pd
import json
import datetime
import time
import os

def fetch_and_save_data():
    print("Step 1: 获取申万(Shenwan)二级行业列表...")
    
    try:
        # 修正接口: 使用 sw_index_second_info 获取申万二级列表
        # 返回列通常为: 行业代码, 行业名称, ...
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
    
    # 遍历 DataFrame 的每一行
    for i, row in sw_boards.iterrows():
        # 注意：不同版本的 akshare 返回的列名可能是中文
        # 通常是 "行业代码" 和 "行业名称"
        code = str(row['行业代码']).split('.')[0] # 确保去掉可能存在的后缀如 .SI
        name = row['行业名称']
        
        print(f"[{i+1}/{total}] 获取 {name} ({code})...", end="", flush=True)
        
        fetched = False
        for attempt in range(max_retries):
            try:
                # 接口: ak.index_hist_sw
                # 参数: symbol=纯数字代码 (如 801010)
                df = ak.index_hist_sw(symbol=code, period="day")
                
                if df is None or df.empty:
                    # 某些行业可能已停止维护或刚上市
                    raise ValueError("Empty Data")

                # 清洗数据
                # 确保日期列格式正确
                if '日期' in df.columns:
                    df['日期'] = pd.to_datetime(df['日期'])
                    df.set_index('日期', inplace=True)
                else:
                    # 防止列名变动
                    df.index = pd.to_datetime(df.index)
                
                df.sort_index(inplace=True)
                
                # 截取时间段
                mask = (df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))
                df_subset = df.loc[mask]
                
                if df_subset.empty:
                    print(f" 无区间数据 - 跳过")
                else:
                    # 只取收盘价
                    data_store[name] = df_subset['收盘']
                    print(f" 成功 ({len(df_subset)}条)")
                
                fetched = True
                break 
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    print(f" 失败") # 不打印详细堆栈以保持日志整洁

        # 暂停以防反爬
        time.sleep(0.5)

    print(f"\nStep 3: 数据清洗与对齐...")

    if not data_store:
        raise ValueError("严重错误：没有获取到任何申万数据！")

    # 1. 对齐所有日期
    df_all = pd.DataFrame(data_store)
    df_all.sort_index(inplace=True)
    
    # 2. 填充缺失值 (ffill保证连续性, 0处理开头)
    df_all.fillna(method='ffill', inplace=True)
    df_all.fillna(0, inplace=True)

    # 3. 提取日期
    common_dates = df_all.index.strftime('%Y-%m-%d').tolist()
    
    # 4. 格式化数据
    final_data_map = {}
    for col in df_all.columns:
        final_data_map[col] = df_all[col].round(4).tolist()

    # 5. 保存
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
