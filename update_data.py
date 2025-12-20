import akshare as ak
import pandas as pd
import json
import datetime
import time
import random

def fetch_and_save_data():
    print("Step 1: 准备获取东财二级行业数据 (最稳定源)...")
    
    # 1. 获取东财行业板块列表
    try:
        # 接口: stock_board_industry_name_em
        # 这几乎是目前唯一能在 GitHub Actions 上稳定跑通的行业接口
        board_list = ak.stock_board_industry_name_em()
        # 过滤掉不需要的行，只要板块名称
        industry_names = board_list['板块名称'].tolist()
        print(f"SUCCESS: 获取到 {len(industry_names)} 个行业板块")
    except Exception as e:
        print(f"ERROR: 获取板块列表失败: {e}")
        raise e

    # 2. 设定时间范围 (最近2年)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730)
    
    # AkShare 东财接口需要的日期格式是 'YYYYMMDD'
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    all_data = {}
    common_dates = []
    success_count = 0
    
    # 3. 循环获取数据
    # 东财接口虽然稳，但也不要并发太快
    print("Step 2: 开始循环获取 K 线数据...")
    
    for i, name in enumerate(industry_names):
        try:
            # 接口: stock_board_industry_hist_em
            # 自动复权: adjust="qfq" (前复权)，这对分析收益率至关重要
            df = ak.stock_board_industry_hist_em(
                symbol=name, 
                start_date=start_str, 
                end_date=end_str, 
                period="日k", 
                adjust="qfq" 
            )
            
            # 数据清洗
            df['日期'] = pd.to_datetime(df['日期'])
            dates = df['日期'].dt.strftime('%Y-%m-%d').tolist()
            closes = df['收盘'].tolist()
            
            if len(dates) == 0:
                continue

            # 建立日期基准 (以第一个获取成功的为准)
            if not common_dates:
                common_dates = dates
            
            # 数据对齐逻辑
            if len(dates) == len(common_dates):
                all_data[name] = closes
                success_count += 1
            else:
                # 简单容错：只允许极小的长度差异 (比如停牌几天)
                # 如果差异很大，直接丢弃，防止图表错位
                if abs(len(dates) - len(common_dates)) < 5:
                    min_len = min(len(dates), len(common_dates))
                    all_data[name] = closes[-min_len:]
                    if len(common_dates) > min_len:
                        common_dates = common_dates[-min_len:]
                    success_count += 1
                else:
                    # 差异太大，跳过
                    # print(f"  [跳过] {name}: 长度不一致 ({len(dates)} vs {len(common_dates)})")
                    pass
            
            # 简单的进度打印
            # if i % 10 == 0:
            #     print(f"  进度: {i}/{len(industry_names)}")

        except Exception as e:
            print(f"  [错误] {name}: {e}")
            continue

    print(f"\nStep 3: 获取结束。成功: {success_count} / {len(industry_names)}")

    if success_count == 0:
        raise ValueError("严重错误：没有获取到任何数据！")

    # 4. 构建 JSON
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dates": common_dates,
        "data": all_data
    }

    # 5. 保存
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    print("SUCCESS: industry_data.json 保存成功")

if __name__ == "__main__":
    fetch_and_save_data()
