import akshare as ak
import pandas as pd
import json
import datetime
import time

def fetch_data_logic(info_df, symbol_col, name_col, is_sw=True):
    """通用抓取逻辑"""
    start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime("%Y%m%d")
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    data_store = {}
    
    for i, (_, row) in enumerate(info_df.iterrows()):
        code = str(row[symbol_col]).split('.')[0]
        name = row[name_col]
        
        print(f"  [{i+1}/{len(info_df)}] 抓取 {name}...", end="", flush=True)
        time.sleep(1.5)  # 遵守 1.5 秒规则
        
        try:
            if is_sw:
                df = ak.index_hist_sw(symbol=code, period="day")
            else:
                # 综合指数使用 A 股指数日频接口
                df = ak.index_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date)
            
            if df is not None and not df.empty:
                # 统一日期列
                date_col = '日期' if '日期' in df.columns else df.index.name
                df[date_col] = pd.to_datetime(df[date_col])
                df.set_index(date_col, inplace=True)
                df.sort_index(inplace=True)
                
                # 截取近两年并只取收盘价
                mask = (df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))
                data_store[name] = df.loc[mask, '收盘']
                print(" 成功")
            else:
                print(" 无数据")
        except Exception:
            print(" 失败")
            
    if not data_store: return None

    # 对齐所有日期，防止停牌导致的数据错位
    df_all = pd.DataFrame(data_store)
    df_all.sort_index(inplace=True)
    df_all.fillna(method='ffill', inplace=True) # 向前填充
    df_all.fillna(0, inplace=True)
    
    return {
        "dates": df_all.index.strftime('%Y-%m-%d').tolist(),
        "data": {col: df_all[col].round(2).tolist() for col in df_all.columns}
    }

def main():
    print("开始更新数据...")
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market": {}, "sw_l1": {}, "sw_l2": {}
    }

    # 1. 综合指数
    print("\n步骤 1: 抓取综合指数")
    market_list = pd.DataFrame([
        {"code": "000001", "name": "上证指数"},
        {"code": "000300", "name": "沪深300"},
        {"code": "399006", "name": "创业板指"},
        {"code": "000905", "name": "中证500"},
        {"code": "000852", "name": "中证1000"},
        {"code": "932000", "name": "中证2000"},
        {"code": "000906", "name": "中证800"},
    ])
    final_json["market"] = fetch_data_logic(market_list, 'code', 'name', False)

    # 2. 申万一级
    print("\n步骤 2: 抓取申万一级行业")
    try:
        sw_l1_info = ak.sw_index_first_info()
        final_json["sw_l1"] = fetch_data_logic(sw_l1_info, '行业代码', '行业名称', True)
    except: print("申万一级列表获取失败")

    # 3. 申万二级
    print("\n步骤 3: 抓取申万二级行业")
    try:
        sw_l2_info = ak.sw_index_second_info()
        final_json["sw_l2"] = fetch_data_logic(sw_l2_info, '行业代码', '行业名称', True)
    except: print("申万二级列表获取失败")

    # 保存文件
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False)
    print("\n数据保存成功：industry_data.json")

if __name__ == "__main__":
    main()
