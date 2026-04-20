import akshare as ak
import pandas as pd
import json
import datetime
import time

def fetch_and_save_data():
    print("Step 1: 获取申万二级行业列表 (用于名称映射)...")
    try:
        sw_boards = ak.sw_index_second_info()
        # 建立 代码 -> 名称 的映射表
        code_to_name = {
            str(row['行业代码']).split('.')[0]: row['行业名称']
            for _, row in sw_boards.iterrows()
        }
        print(f"SUCCESS: 获取到 {len(code_to_name)} 个申万二级行业")
    except Exception as e:
        print(f"ERROR: 获取行业列表失败: {e}")
        return

    # 时间范围: 最近2年，按季度分块请求，避免单次请求超时
    end_dt   = datetime.datetime.now()
    start_dt = end_dt - datetime.timedelta(days=730)

    # 生成季度分块
    def generate_quarters(start, end):
        chunks = []
        cur = start
        while cur < end:
            next_q = min(cur + datetime.timedelta(days=90), end)
            chunks.append((cur.strftime("%Y%m%d"), next_q.strftime("%Y%m%d")))
            cur = next_q + datetime.timedelta(days=1)
        return chunks

    quarters = generate_quarters(start_dt, end_dt)
    print(f"Step 2: 按季度分块拉取数据，共 {len(quarters)} 个时间段...")

    all_frames = []
    for i, (s, e) in enumerate(quarters):
        print(f"  [{i+1}/{len(quarters)}] {s} ~ {e}...", end="", flush=True)
        for attempt in range(3):
            try:
                df = ak.index_analysis_daily_sw(
                    symbol="二级行业",
                    start_date=s,
                    end_date=e
                )
                if df is None or df.empty:
                    raise ValueError("Empty response")
                all_frames.append(df)
                print(f" {len(df)} 行")
                break
            except Exception as ex:
                if attempt < 2:
                    print(f" 重试({attempt+1})...", end="", flush=True)
                    time.sleep(3)
                else:
                    print(f" 失败: {ex}")
        time.sleep(1.0)  # 礼貌间隔

    if not all_frames:
        print("ERROR: 没有获取到任何数据")
        return

    print("\nStep 3: 数据清洗与对齐...")
    big_df = pd.concat(all_frames, ignore_index=True)

    # index_analysis_daily_sw 返回列: 指数代码, 指数名称, 发布日期, 收盘指数, ...
    big_df['发布日期'] = pd.to_datetime(big_df['发布日期'])
    big_df['收盘指数'] = pd.to_numeric(big_df['收盘指数'], errors='coerce')
    big_df.drop_duplicates(subset=['指数代码', '发布日期'], inplace=True)

    # 转换为宽表: 行=日期, 列=行业名称
    pivot = big_df.pivot(index='发布日期', columns='指数名称', values='收盘指数')
    pivot.sort_index(inplace=True)
    pivot.ffill(inplace=True)
    pivot.fillna(0, inplace=True)

    common_dates = pivot.index.strftime('%Y-%m-%d').tolist()
    final_data_map = {col: pivot[col].round(2).tolist() for col in pivot.columns}

    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Shenwan Level 2 (AkShare - index_analysis_daily_sw)",
        "dates": common_dates,
        "data": final_data_map
    }

    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False)

    print(f"SUCCESS: industry_data.json 保存完成。")
    print(f"  交易日: {len(common_dates)} 天")
    print(f"  行业数: {len(final_data_map)} 个")

if __name__ == "__main__":
    fetch_and_save_data()
