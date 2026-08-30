"""Update Shenwan level-2 closes without inventing a trading date.

The daily-analysis endpoint is authoritative because every row carries its own
publication date. After the market closes, the real-time endpoint can be used
as an early source, but only on that same Shanghai trading day and only with
``最新价``. ``昨收盘`` must never be labelled as today's close.
"""

import datetime as dt
import json
import os
import time
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd

DATA_FILE = "industry_data.json"
MAX_DAYS = 500
MIN_INDUSTRIES = 100
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _read_existing():
    if not os.path.exists(DATA_FILE):
        return None
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"读取旧文件失败，将重新获取: {exc}")
        return None


def _trade_dates():
    calendar = ak.tool_trade_date_hist_sina()
    return [
        d for d in pd.to_datetime(calendar["trade_date"], errors="coerce").dt.date
        if pd.notna(d)
    ]


def _latest_publishable_trade_date(now, dates):
    today = now.date()
    market_closed = now.time() >= dt.time(15, 5)
    eligible = [
        d for d in dates
        if pd.notna(d) and (d < today or (d == today and market_closed))
    ]
    if not eligible:
        raise RuntimeError("无法确定最近一个已收盘交易日")
    return max(eligible)


def _daily_analysis(start_date, end_date):
    for attempt in range(3):
        try:
            df = ak.index_analysis_daily_sw(
                symbol="二级行业",
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
            )
            if df.empty or "发布日期" not in df.columns:
                raise RuntimeError("日报表尚未返回数据")
            df = df[["发布日期", "指数名称", "收盘指数"]].copy()
            df["发布日期"] = pd.to_datetime(df["发布日期"], errors="coerce").dt.normalize()
            df["收盘指数"] = pd.to_numeric(df["收盘指数"], errors="coerce")
            df.dropna(subset=["发布日期", "指数名称", "收盘指数"], inplace=True)
            print(
                f"日报表返回 {len(df)} 行，日期 "
                f"{df['发布日期'].min().date()} 至 {df['发布日期'].max().date()}"
            )
            return df
        except Exception as exc:
            print(f"日报表重试 {attempt + 1}/3: {exc}")
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return pd.DataFrame(columns=["发布日期", "指数名称", "收盘指数"])


def _complete_dates(df, expected_date):
    if df.empty:
        return df
    df = df[df["发布日期"].dt.date <= expected_date].copy()
    counts = df.groupby("发布日期")["指数名称"].nunique()
    accepted = counts[counts >= MIN_INDUSTRIES].index
    rejected = counts[counts < MIN_INDUSTRIES]
    for date, count in rejected.items():
        print(f"忽略不完整日报 {date.date()}：仅 {count} 个行业")
    return df[df["发布日期"].isin(accepted)].copy()


def _same_day_realtime_close(now, trade_dates):
    """Return today's final real-time prices, never a guessed prior-day row."""
    today = now.date()
    if today not in trade_dates or now.time() < dt.time(15, 5):
        return pd.DataFrame(columns=["发布日期", "指数名称", "收盘指数"])

    for attempt in range(3):
        try:
            df = ak.index_realtime_sw(symbol="二级行业")
            required = {"指数名称", "最新价", "昨收盘"}
            if df.empty or not required.issubset(df.columns):
                raise RuntimeError("实时接口缺少最新价或昨收盘字段")

            out = df[["指数名称", "最新价", "昨收盘"]].copy()
            out["最新价"] = pd.to_numeric(out["最新价"], errors="coerce")
            out["昨收盘"] = pd.to_numeric(out["昨收盘"], errors="coerce")
            out.dropna(subset=["指数名称", "最新价", "昨收盘"], inplace=True)
            moved = (out["最新价"] - out["昨收盘"]).abs() >= 0.005
            if len(out) < MIN_INDUSTRIES:
                raise RuntimeError(f"实时接口仅返回 {len(out)} 个有效行业")
            if int(moved.sum()) < 10:
                raise RuntimeError("最新价仍与昨收盘基本相同，申万数据尚未刷新")

            result = out[["指数名称", "最新价"]].rename(columns={"最新价": "收盘指数"})
            result["发布日期"] = pd.Timestamp(today)
            print(f"实时收盘可用：{len(result)} 个行业，日期 {today}")
            return result[["发布日期", "指数名称", "收盘指数"]]
        except Exception as exc:
            print(f"实时接口重试 {attempt + 1}/3: {exc}")
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return pd.DataFrame(columns=["发布日期", "指数名称", "收盘指数"])


def _to_long_frame(data):
    rows = []
    if not data:
        return pd.DataFrame(columns=["发布日期", "指数名称", "收盘指数"])
    for name, values in data.get("data", {}).items():
        for date, value in zip(data.get("dates", []), values):
            rows.append({"发布日期": date, "指数名称": name, "收盘指数": value})
    return pd.DataFrame(rows)


def _drop_flat_tail(frame):
    """Remove impossible synthetic tail rows such as the bad 2026-08-28 row."""
    if frame.empty:
        return frame
    pivot = frame.pivot(index="发布日期", columns="指数名称", values="收盘指数").sort_index()
    while len(pivot) >= 2:
        previous = pivot.iloc[-2]
        latest = pivot.iloc[-1]
        comparable = previous.notna() & latest.notna()
        if comparable.sum() < MIN_INDUSTRIES:
            break
        same_ratio = (previous[comparable] == latest[comparable]).mean()
        if same_ratio < 0.98:
            break
        bad_date = pivot.index[-1]
        print(f"删除疑似伪造的全平日期：{bad_date.date()}（重复率 {same_ratio:.1%}）")
        pivot = pivot.iloc[:-1]
    return pivot.stack().rename("收盘指数").reset_index()


def _payload_without_metadata(data):
    if not data:
        return None
    return {"dates": data.get("dates", []), "data": data.get("data", {})}


def fetch_and_save_data_incremental(now=None):
    existing = _read_existing()
    now = now or dt.datetime.now(SHANGHAI)
    trade_dates = _trade_dates()
    expected_date = _latest_publishable_trade_date(now, trade_dates)

    existing_dates = existing.get("dates", []) if existing else []
    if existing_dates:
        last_date = pd.to_datetime(existing_dates[-1]).date()
        start_date = min(last_date - dt.timedelta(days=10), expected_date)
        print(f"现有最后日期：{last_date}；本次核验至：{expected_date}")
    else:
        start_date = expected_date - dt.timedelta(days=365)
        print(f"本地无数据，初始化至：{expected_date}")

    daily = _complete_dates(_daily_analysis(start_date, expected_date), expected_date)
    candidates = daily
    daily_dates = set(daily["发布日期"].dt.date) if not daily.empty else set()
    if expected_date == now.date() and expected_date not in daily_dates:
        realtime = _same_day_realtime_close(now, trade_dates)
        if not realtime.empty:
            candidates = pd.concat([candidates, realtime], ignore_index=True)

    old_df = _to_long_frame(existing)
    if not old_df.empty:
        old_df["发布日期"] = pd.to_datetime(old_df["发布日期"], errors="coerce").dt.normalize()
        old_df["收盘指数"] = pd.to_numeric(old_df["收盘指数"], errors="coerce")
        old_df.dropna(subset=["发布日期", "指数名称", "收盘指数"], inplace=True)
        old_df = _drop_flat_tail(old_df)

    if candidates.empty and old_df.empty:
        raise RuntimeError("日报表和实时接口均未返回可发布数据")

    combined = pd.concat([old_df, candidates], ignore_index=True)
    combined.drop_duplicates(subset=["发布日期", "指数名称"], keep="last", inplace=True)
    pivot = combined.pivot(index="发布日期", columns="指数名称", values="收盘指数").sort_index()
    pivot.ffill(inplace=True)
    pivot = pivot.tail(MAX_DAYS)

    dates = pivot.index.strftime("%Y-%m-%d").tolist()
    final_json = {
        "last_update": now.strftime("%Y-%m-%d %H:%M:%S"),
        "data_as_of": dates[-1],
        "source": "SWS dated daily analysis; same-day post-close latest price fallback",
        "dates": dates,
        "data": {
            name: [None if pd.isna(v) else round(float(v), 2) for v in pivot[name]]
            for name in pivot.columns
        },
    }

    if _payload_without_metadata(final_json) == _payload_without_metadata(existing):
        print(f"申万尚无新数据，文件保持不变；当前日期 {dates[-1]}")
        return False

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False)
    print(f"SUCCESS: 数据已更新至 {dates[-1]}，共 {len(dates)} 个交易日、{len(final_json['data'])} 个行业")
    return True


if __name__ == "__main__":
    fetch_and_save_data_incremental()
