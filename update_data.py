import akshare as ak
import pandas as pd
import json
import datetime
import time
import os

def fetch_and_save_data():
    print("Step 1: 获取申万(Shenwan)二级行业列表...")
    
    try:
        # 获取申万二级行业分类列表
        # index_code: 801010, index_name: 农林牧渔...
        sw_boards = ak.index_classify_sw(level="L2")
        print(f"SUCCESS: 获取到 {len(sw_boards)} 个申万二级行业")
    except Exception as e:
        print(f"ERROR: 获取申万行业列表失败。可能接口变动或网络问题。")
        # 备用方案：如果列表获取失败，可以硬编码几个测试，或者直接抛出
        raise e

    # 设定时间范围 (最近2年，减少数据量避免超时)
    # 申万接口比较慢，建议适当缩短时间，或者GitHub Actions中增加超时设置
    start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime("%Y%m%d")
    end_date = datetime.datetime.now().strftime("%Y%m%d")

    # 用于存储所有行业的临时字典: { "行业名": Series(index=Date, value=Close) }
    data_store = {}
    
    print("Step 2: 循环获取行业历史数据 (速度较慢，请耐心等待)...")
    
    # 申万接口不稳定，增加重试逻辑
    max_retries = 3
    
    total = len(sw_boards)
    for i, row in enumerate(sw_boards.itertuples()):
        code = row.index_code
        name = row.index_name
        
        # 简单进度显示
        print(f"[{i+1}/{total}] 获取 {name} ({code})...", end="", flush=True)
        
        fetched = False
        for attempt in range(max_retries):
            try:
                # 接口: ak.index_hist_sw
                # 来源: 申万宏源官网
                df = ak.index_hist_sw(symbol=code, period="day")
                
                if df is None or df.empty:
                    raise ValueError("Empty Data")

                # 数据清洗
                # akshare返回列名通常为: 日期, 开盘, 最高, 最低, 收盘, ...
                df['日期'] = pd.to_datetime(df['日期'])
                df.set_index('日期', inplace=True)
                df.sort_index(inplace=True)
                
                # 截取时间段
                mask = (df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))
                df_subset = df.loc[mask]
                
                if df_subset.empty:
                    print(f" 无区间数据 - 跳过")
                else:
                    # 只取收盘价，存入字典
                    data_store[name] = df_subset['收盘']
                    print(f" 成功 ({len(df_subset)}条)")
                
                fetched = True
                break # 成功则跳出重试
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2) # 失败等待2秒
                else:
                    print(f" 失败: {e}")

        # 稍微暂停，避免触发申万官网的反爬频率限制
        time.sleep(1)

    print(f"\nStep 3: 数据清洗与对齐...")

    if not data_store:
        raise ValueError("严重错误：没有获取到任何申万数据！请检查网络或AkShare接口状态。")

    # --- 核心对齐逻辑 ---
    # 1. 将字典转换为 DataFrame，列名就是行业名，索引是日期
    #    Pandas 会自动合并所有日期，某个行业某天没数据会填 NaN
    df_all = pd.DataFrame(data_store)
    
    # 2. 按日期排序
    df_all.sort_index(inplace=True)
    
    # 3. 处理缺失值 (NaN)
    #    策略：使用前值填充 (ffill)，如果前面也没值(刚上市)，填 0 或 第一个有效值
    #    这样保证了所有行业的数组长度完全一致，对应同一个 dates 数组
    df_all.fillna(method='ffill', inplace=True)
    df_all.fillna(0, inplace=True) # 处理最开始的空值

    # 4. 提取公共日期列表
    common_dates = df_all.index.strftime('%Y-%m-%d').tolist()
    
    # 5. 构建最终字典
    final_data_map = {}
    for col in df_all.columns:
        # 将 numpy 数组转为 list，保留浮点数精度
        # round(4) 减小文件体积
        final_data_map[col] = df_all[col].round(4).tolist()

    # 6. 构建 JSON
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Shenwan Level 2 (AkShare)",
        "dates": common_dates,
        "data": final_data_map
    }

    # 7. 保存
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"SUCCESS: industry_data.json 保存成功。包含 {len(common_dates)} 个交易日，{len(final_data_map)} 个行业。")

if __name__ == "__main__":
    fetch_and_save_data()
