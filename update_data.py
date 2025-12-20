import akshare as ak
import pandas as pd
import json
import datetime
import os
import sys

def fetch_and_save_data():
    print("Step 1: 准备开始获取数据...")
    print(f"AkShare Version: {ak.__version__}")
    
    # 1. 获取行业列表
    try:
        print("Step 2: 正在请求东财行业列表 (stock_board_industry_name_em)...")
        board_list = ak.stock_board_industry_name_em()
        
        if board_list is None or board_list.empty:
            raise ValueError("获取到的板块列表为空！")
            
        industry_names = board_list['板块名称'].tolist()
        print(f"SUCCESS: 获取到 {len(industry_names)} 个行业板块")
    except Exception as e:
        print(f"ERROR: 获取板块列表失败。错误原因: {e}")
        # 这里直接抛出异常，让 Workflow 变红，而不是继续运行
        raise e

    # 2. 设定时间范围
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730) # 最近2年
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    all_data = {}
    common_dates = []
    success_count = 0

    print("Step 3: 开始循环获取具体K线数据...")
    
    # 为了防止超时，我们先试着只获取前 5 个测试一下链接稳定性
    # 如果你可以正常运行，可以去掉 [:5]
    # target_list = industry_names[:5] 
    target_list = industry_names 

    for i, name in enumerate(target_list):
        try:
            # 简单的进度打印
            if i % 10 == 0:
                print(f"进度: {i}/{len(target_list)}...")

            df = ak.stock_board_industry_hist_em(
                symbol=name, 
                start_date=start_str, 
                end_date=end_str, 
                period="日k", 
                adjust="qfq"
            )
            
            dates = df['日期'].tolist()
            closes = df['收盘'].tolist()
            
            if not common_dates and len(dates) > 0:
                common_dates = dates
            
            # 简单数据对齐：只保存长度一致的数据，或者不为空的数据
            if len(dates) > 0:
                all_data[name] = closes
                success_count += 1
                
        except Exception as e:
            # 个别行业失败不影响整体，只打印警告
            print(f"Warning: 获取 {name} 失败: {e}")
            continue

    print(f"Step 4: 数据获取结束。成功: {success_count}, 失败: {len(target_list) - success_count}")

    if success_count == 0:
        raise ValueError("没有获取到任何行业的数据，请检查网络或接口是否可用！")

    # 4. 构建最终 JSON
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dates": common_dates,
        "data": all_data
    }

    # 5. 保存文件
    print("Step 5: 正在写入 industry_data.json ...")
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    # 再次确认文件是否存在
    if os.path.exists("industry_data.json"):
        print(f"SUCCESS: 文件已生成，大小: {os.path.getsize('industry_data.json')/1024:.2f} KB")
    else:
        raise FileNotFoundError("ERROR: 文件写入看似成功，但在磁盘上找不到！")

if __name__ == "__main__":
    fetch_and_save_data()
