import akshare as ak
import pandas as pd
import json
import datetime
import os
import time

def fetch_and_save_data():
    print("Step 1: 准备获取申万二级行业数据...")
    
    # 1. 获取申万二级行业列表
    try:
        print("正在获取申万二级行业代码列表 (ak.index_class_sw)...")
        # 这个接口需要 openpyxl 支持
        sw_class = ak.index_class_sw(level="L2")
        print(f"SUCCESS: 获取到 {len(sw_class)} 个申万二级行业")
    except Exception as e:
        print(f"ERROR: 获取行业列表失败。可能原因: 1.网络超时 2.缺少openpyxl库。错误信息: {e}")
        raise e

    # 2. 设定时间范围
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730) 
    
    # Legule 接口需要的日期格式是 'YYYY-MM-DD' 或 datetime
    # 我们将在循环中处理
    
    all_data = {}
    common_dates = []
    success_count = 0
    
    # 3. 循环获取数据 (改用 Legule 接口)
    # 我们只取前 150 个，防止异常
    target_list = sw_class
    
    print("Step 2: 开始循环获取具体指数数据 (Source: Legule)...")

    for index, row in target_list.iterrows():
        name = row['index_name']
        code = row['index_code'] # 格式如 801010
        
        try:
            # 接口: ak.sw_index_daily_indicator
            # 数据源: 乐咕乐股 (比新浪稳定)
            # 包含字段: date, close, pe, pb...
            df = ak.sw_index_daily_indicator(symbol=code, start_date="20200101", end_date=datetime.datetime.now().strftime("%Y%m%d"), data_type="Day")
            
            if df is None or df.empty:
                print(f"警告: {name} ({code}) 返回空数据")
                continue

            # 数据清洗
            # 乐咕乐股返回的日期通常是 date 类型或字符串
            df['date'] = pd.to_datetime(df['date'])
            
            # 筛选时间范围
            mask = (df['date'] >= start_date) & (df['date'] <= end_date)
            df_filtered = df.loc[mask].copy()
            
            # 只要日期和收盘价
            dates = df_filtered['date'].dt.strftime('%Y-%m-%d').tolist()
            closes = df_filtered['close'].tolist()
            
            if len(dates) == 0:
                continue

            # 以第一个成功的数据作为基准
            if not common_dates:
                common_dates = dates
            
            # 数据对齐 (简单长度判断)
            if len(dates) == len(common_dates):
                all_data[name] = closes
                success_count += 1
            else:
                # 长度不一致时，尝试截取对齐（以最短的为准，或者丢弃）
                # 为了图表稳定，这里如果差异不大(比如少几天)，可以考虑对齐逻辑
                # 但简单起见，暂且跳过严重不一致的
                # print(f"跳过 {name}: 长度不一致 {len(dates)} vs {len(common_dates)}")
                
                # 进阶对齐：如果是最近上市的，数据肯定少，前端会自动处理 null
                # 这里我们只存完全匹配的，或者你可以放宽限制
                pass

            # 稍微延时，虽然Legule比较耐抗
            time.sleep(0.1)

        except Exception as e:
            print(f"获取 {name} ({code}) 失败: {e}")
            continue

    print(f"Step 3: 数据获取结束。成功: {success_count} 个")

    if success_count == 0:
        raise ValueError("没有获取到任何数据！请检查网络或接口状态。")

    # 4. 构建 JSON
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dates": common_dates,
        "data": all_data
    }

    # 5. 保存
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    print("SUCCESS: industry_data.json 文件生成成功")

if __name__ == "__main__":
    fetch_and_save_data()
