"""Fast Shenwan level-2 industry close updater.

The daily-analysis endpoint can be published late. The SWS real-time endpoint
already exposes the previous trading day's close as ``昨收盘`` shortly after
the market closes, so it is used as the fast path.
"""

import datetime as dt
import json
import os
import time

import akshare as ak
import pandas as pd

DATA_FILE = "industry_data.json"
MAX_DAYS = 500


def _read_existing():
    if not os.path.exists(DATA_FILE):
        return None
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"读取旧文件失败，将重新获取: {exc}")
        return None


def _previous_trade_date(today):
    calendar = ak.tool_trade_date_hist_sina()
    dates = pd.to_datetime(calendar["trade_date"], errors="coerce").dt.date
    previous = [d for d in dates if pd.notna(d) and d < today]
    if not previous:
        raise RuntimeError("无法确定上一个交易日")
    return max(previous)


def _fast_previous_close(target_date):
    for attempt in range(3):
        try:
            df = ak.index_realtime_sw(symbol="二级行业")
            if df.empty:
                raise RuntimeError("实时接口返回空数据")
            out = df[["指数名称", "昨收盘"]].copy()
            out["收盘指数"] = pd.to_numeric(out["昨收盘"], errors="coerce")
            out["发布日期"] = pd.Timestamp(target_date)
            out = out[["发布日期", "指数名称", "收盘指数"]].dropna(subset=["收盘指数"])
            if len(out) < 100:
                raise RuntimeError(f"实时接口只返回 {len(out)} 个行业，疑似未完成发布")
            print(f"快速路径成功：{len(out)} 个二级行业，目标日期 {target_date}")
            return out
        except Exception as exc:
            print(f"实时接口重试 {attempt + 1}/3: {exc}")
            time.sleep(3 * (attempt + 1))
    return pd.DataFrame()


def _daily_analysis_fallback(start_date, end_date):
    try:
        df = ak.index_analysis_daily_sw(
            symbol="二级行业",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
        )
        if df.empty or "发布日期" not in df.columns:
            return pd.DataFrame()
        df["发布日期"] = pd.to_datetime(df["发布日期"], errors="coerce")
        df["收盘指数"] = pd.to_numeric(df["收盘指数"], errors="coerce")
        return df[["发布日期", "指数名称", "收盘指数"]].dropna(subset=["发布日期", "收盘指数"])
    except Exception as exc:
        print(f"日报表兜底失败: {exc}")
        return pd.DataFrame()


def _to_long_frame(data):
    rows = []
    for name, values in data.get("data", {}).items():
        for date, value in zip(data.get("dates", []), values):
            rows.append({"发布日期": date, "指数名称": name, "收盘指数": value})
    return pd.DataFrame(rows)


def fetch_and_save_data_incremental():
    existing = _read_existing()
    today = dt.datetime.now().date()
    target_date = _previous_trade_date(today)

    if existing:
        print(f"检测到本地数据，最后日期为: {existing.get('dates', ['未知'])[-1]}")
    else:
        print("本地无数据，执行初始化（日报表近一年）...")

    # Fast path: yesterday's close is available before the daily report.
    new_df = _fast_previous_close(target_date)

    # Keep the old endpoint as a fallback for historical recovery/manual runs.
    if new_df.empty:
        last_date = (
            pd.to_datetime(existing["dates"][-1]).date()
            if existing and existing.get("dates")
            else today - dt.timedelta(days=365)
        )
        new_df = _daily_analysis_fallback(last_date - dt.timedelta(days=3), today)

    if new_df.empty and existing:
        print("没有拿到新数据，保留现有文件")
        return
    if new_df.empty:
        raise RuntimeError("实时接口和日报表接口均未返回数据")

    old_df = _to_long_frame(existing) if existing else pd.DataFrame(
        columns=["发布日期", "指数名称", "收盘指数"]
    )
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined["发布日期"] = pd.to_datetime(combined["发布日期"], errors="coerce")
    combined["收盘指数"] = pd.to_numeric(combined["收盘指数"], errors="coerce")
    combined.dropna(subset=["发布日期", "指数名称", "收盘指数"], inplace=True)
    combined.drop_duplicates(subset=["发布日期", "指数名称"], keep="last", inplace=True)

    pivot = combined.pivot(index="发布日期", columns="指数名称", values="收盘指数").sort_index()
    pivot.ffill(inplace=True)
    pivot.fillna(0, inplace=True)
    pivot = pivot.tail(MAX_DAYS)

    dates = pivot.index.strftime("%Y-%m-%d").tolist()
    final_json = {
        "last_update": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_as_of": dates[-1],
        "source": "SWS level-2 previous close (fast path) + daily analysis fallback",
        "dates": dates,
        "data": {name: pivot[name].round(2).tolist() for name in pivot.columns},
    }
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False)
    print(f"SUCCESS: 数据已更新至 {dates[-1]}，共 {len(dates)} 个交易日、{len(final_json['data'])} 个行业")


if __name__ == "__main__":
    fetch_and_save_data_incremental()
