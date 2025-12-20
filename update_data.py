import akshare as ak
import pandas as pd
import json
import datetime
import os
import time

def fetch_and_save_data():
    print("Step 1: 准备获取申万二级行业数据...")
    
    # 1. 获取申万二级行业列表
    # 申万行业分为 L1(一级), L2(二级), L3(三级)
    try:
        print("正在获取申万二级行业代码列表...")
        # 获取申万分类标准（最新版）
        sw_class = ak.index_class_sw(level="L2")
        # sw_class 通常包含: index_code, index_name
        # 我们需要清洗一下数据，确保拿到的是 code 和 name
        print(f"获取到 {len(sw_class)} 个申万二级行业")
    except Exception as e:
        print(f"ERROR: 获取行业列表失败: {e}")
        raise e

    # 2. 设定时间范围
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730) # 最近2年
    # 注意：申万接口通常不需要 '20230101' 这种格式，或者内部会自动处理，我们保持 datetime 对象备用
    
    all_data = {}
    common_dates = []
    success_count = 0
    
    # 3. 循环获取数据
    # 申万二级行业数量较多（100+），且接口可能有限流，我们加一点延时
    for index, row in sw_class.iterrows():
        name = row['index_name']
        code = row['index_code'] # 例如 801010
        
        try:
            # print(f"正在获取: {name} ({code}) ...")
            
            # 使用 AkShare 的申万指数历史接口
            # 注意：ak.index_hist_sw(symbol="801010")
            df = ak.index_hist_sw(symbol=code, period="day")
            
            # 数据清洗：确保日期格式统一
            # 申万接口返回的日期通常是 datetime 或 string
            df['日期'] = pd.to_datetime(df['日期'])
            
            # 筛选时间范围
            mask = (df['日期'] >= start_date) & (df['日期'] <= end_date)
            df_filtered = df.loc[mask].copy()
            
            # 转为字符串列表用于 JSON
            dates = df_filtered['日期'].dt.strftime('%Y-%m-%d').tolist()
            closes = df_filtered['收盘'].tolist()
            
            # 必须保证数据不为空
            if len(dates) == 0:
                continue

            # 以第一个成功获取的数据作为日期基准（通常指数的交易日是一样的）
            if not common_dates:
                common_dates = dates
            
            # 数据对齐检查（简单版）：长度一致才收录
            # 复杂版应该用 merge，但在 GitHub Actions 环境下，只要是指数，交易日基本一致
            if len(dates) == len(common_dates):
                all_data[name] = closes
                success_count += 1
            else:
                # 如果长度对不上（比如新发布的指数），为了前端画图不报错，暂时跳过
                # 或者截取对齐，这里选择跳过以保证稳定性
                print(f"跳过 {name}: 数据长度不一致 ({len(dates)} vs {len(common_dates)})")

            # 稍微休息一下，防止被封 IP
            # time.sleep(0.2) 

        except Exception as e:
            print(f"获取 {name} ({code}) 失败: {e}")
            continue

    print(f"Step 4: 数据获取结束。成功: {success_count} 个")

    if success_count == 0:
        raise ValueError("没有获取到任何数据！")

    # 4. 构建 JSON
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dates": common_dates,
        "data": all_data
    }

    # 5. 保存
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    print("SUCCESS: industry_data.json 已更新为申万二级行业数据")

if __name__ == "__main__":
    fetch_and_save_data()
