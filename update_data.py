import akshare as ak
import pandas as pd
import json
import datetime
import os

def fetch_and_save_data():
    print("开始获取东财行业数据...")
    
    # 1. 获取行业列表
    try:
        board_list = ak.stock_board_industry_name_em()
        industry_names = board_list['板块名称'].tolist()
        print(f"获取到 {len(industry_names)} 个行业板块")
    except Exception as e:
        print(f"获取板块列表失败: {e}")
        return

    # 2. 设定时间范围 (最近 2 年，减少文件体积)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730)
    
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    all_data = {}
    common_dates = []

    # 3. 循环获取数据
    # 这里为了演示稳定性，我们获取前几名，实际部署建议全部获取
    # 如果想获取全部，直接用 for name in industry_names:
    success_count = 0
    
    for i, name in enumerate(industry_names):
        try:
            # print(f"正在获取: {name} ({i+1}/{len(industry_names)})")
            df = ak.stock_board_industry_hist_em(
                symbol=name, 
                start_date=start_str, 
                end_date=end_str, 
                period="日k", 
                adjust="qfq"
            )
            
            # 只要日期和收盘价
            # 日期格式化为 YYYY-MM-DD
            dates = df['日期'].tolist()
            closes = df['收盘'].tolist()
            
            # 记录第一个成功获取的日期序列作为基准
            if not common_dates:
                common_dates = dates
            
            # 数据对齐（简单处理：假设东财返回的交易日是一致的，实际建议用 pandas merge）
            if len(dates) == len(common_dates):
                all_data[name] = closes
                success_count += 1
            else:
                # 长度不一致时，稍微复杂点，这里为了脚本简单，先跳过或只取后段
                # 生产环境建议用 DataFrame join
                pass
                
        except Exception as e:
            print(f"获取 {name} 失败: {e}")
            continue

    print(f"成功获取 {success_count} 个行业数据")

    # 4. 构建最终 JSON 结构
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dates": common_dates,
        "data": all_data
    }

    # 5. 保存文件
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':')) # 压缩格式
    
    print("数据已保存至 industry_data.json")

if __name__ == "__main__":
    fetch_and_save_data()