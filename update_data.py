import akshare as ak
import pandas as pd
import json
import datetime
import os
import time

def fetch_and_save_data_incremental():
    file_path = "industry_data.json"
    now_dt = datetime.datetime.now()
    
    # --- 1. 读取本地现有数据 ---
    existing_data = None
    last_date_str = None
    
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            if existing_data and "dates" in existing_data and len(existing_data["dates"]) > 0:
                last_date_str = existing_data["dates"][-1]
                print(f"检测到本地数据，最后日期为: {last_date_str}")
        except Exception as e:
            print(f"读取旧文件失败或文件损坏，将重新全量获取: {e}")

    # --- 2. 确定请求的时间范围 ---
    if last_date_str:
        # 如果有本地数据，从最后一天开始（多取几天以防万一，后面对齐会去重）
        start_dt = datetime.datetime.strptime(last_date_str, "%Y-%m-%d") - datetime.timedelta(days=3)
    else:
        # 如果没有本地数据，初始化获取近 1 年（365天）
        print("本地无数据，执行初始化（获取近1年数据）...")
        start_dt = now_dt - datetime.timedelta(days=365)

    start_date_param = start_dt.strftime("%Y%m%d")
    end_date_param = now_dt.strftime("%Y%m%d")

    if start_date_param == end_date_param and last_date_str:
        print("数据已是最新，无需更新。")
        return

    # --- 3. 调用 API 获取新数据 ---
    print(f"正在拉取 {start_date_param} 到 {end_date_param} 的增量数据...")
    new_df = pd.DataFrame()
    for attempt in range(3):
        try:
            new_df = ak.index_analysis_daily_sw(
                symbol="二级行业",
                start_date=start_date_param,
                end_date=end_date_param
            )
            if not new_df.empty:
                break
        except Exception as e:
            print(f"重试 {attempt+1}/3: {e}")
            time.sleep(2)

    if new_df.empty:
        print("未获取到新数据，可能今日尚未开盘或接口维护。")
        return

    # --- 4. 格式化新数据 ---
    new_df['发布日期'] = pd.to_datetime(new_df['发布日期'])
    new_df['收盘指数'] = pd.to_numeric(new_df['收盘指数'], errors='coerce')
    # 只保留必要的列
    new_df = new_df[['发布日期', '指数名称', '收盘指数']]

    # --- 5. 合并新旧数据 ---
    if existing_data:
        # 将 JSON 转回 DataFrame 格式
        old_rows = []
        old_dates = existing_data["dates"]
        for name, values in existing_data["data"].items():
            for d, v in zip(old_dates, values):
                old_rows.append({'发布日期': d, '指数名称': name, '收盘指数': v})
        
        old_df = pd.DataFrame(old_rows)
        old_df['发布日期'] = pd.to_datetime(old_df['发布日期'])
        
        # 合并
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    # --- 6. 数据清洗与对齐 ---
    # 去重：按日期和行业名称去重，保留最后一次出现的记录
    combined_df.drop_duplicates(subset=['发布日期', '指数名称'], keep='last', inplace=True)
    
    # 转换为宽表 (行是日期，列是行业)
    pivot = combined_df.pivot(index='发布日期', columns='指数名称', values='收盘指数')
    pivot.sort_index(inplace=True)
    
    # 填充缺失值（处理新上市或停牌行业）
    pivot.ffill(inplace=True)
    pivot.fillna(0, inplace=True)

    # 限制数据长度：只保留最近 2 年的数据，防止 JSON 文件无限变大
    # 2年大约 500 个交易日
    if len(pivot) > 500:
        pivot = pivot.tail(500)

    # --- 7. 保存为 JSON ---
    common_dates = pivot.index.strftime('%Y-%m-%d').tolist()
    final_data_map = {col: pivot[col].round(2).tolist() for col in pivot.columns}

    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Shenwan Level 2 (Incremental)",
        "dates": common_dates,
        "data": final_data_map
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False)

    print(f"SUCCESS: 数据已更新至 {common_dates[-1]}")
    print(f"本次更新后共包含 {len(common_dates)} 个交易日的数据。")

if __name__ == "__main__":
    fetch_and_save_data_incremental()
