import akshare as ak
import pandas as pd
import json
import datetime
import time
import random
import requests
import traceback
import sys

# ==========================================
# 申万二级行业代码表 (硬编码，确保字典无误)
# ==========================================
SW_L2_DICT = {
    "801011": "农产品加工", "801012": "农业综合", "801013": "种植业", "801014": "渔业", "801015": "林业", "801016": "饲料", "801017": "畜禽养殖", "801018": "辅材及农机",
    "801031": "煤炭开采", "801032": "焦炭", "801041": "石油开采", "801051": "化学原料", "801053": "化学制品", "801054": "化学纤维", "801055": "塑料", "801056": "橡胶", "801057": "农化制品",
    "801081": "半导体", "801082": "元件", "801083": "光学光电子", "801084": "其他电子", "801085": "电子化学品", "801086": "消费电子",
    "801111": "家电零部件", "801112": "白色家电", "801113": "黑色家电", "801114": "小家电", "801115": "照明设备", "801116": "厨卫电器",
    "801121": "食品加工", "801122": "白酒", "801123": "饮料乳品", "801124": "调味发酵品", "801125": "非白酒",
    "801131": "纺织制造", "801132": "服装家纺", "801133": "饰品",
    "801141": "造纸", "801142": "包装印刷", "801143": "家居用品", "801144": "文娱用品",
    "801151": "化学制药", "801152": "生物制品", "801153": "医疗器械", "801154": "医药商业", "801155": "中药", "801156": "医疗服务",
    "801161": "电力", "801162": "水务及燃气", "801163": "环保工程",
    "801171": "港口", "801172": "高速公路", "801173": "公交", "801174": "航空机场", "801175": "航运", "801176": "铁路公路", "801177": "物流",
    "801181": "房地产开发", "801182": "房地产服务",
    "801201": "旅游零售", "801202": "酒店餐饮", "801203": "旅游及景区", "801204": "教育", "801205": "体育",
    "801211": "银行", "801212": "国有大型银行", "801213": "股份制银行", "801214": "城商行", "801215": "农商行",
    "801221": "证券", "801222": "保险", "801223": "多元金融",
    "801711": "建筑施工", "801712": "工程咨询", "801713": "装修装饰",
    "801721": "房屋建设", "801722": "装修建材", "801723": "水泥", "801724": "玻璃玻纤", "801725": "其他建材",
    "801731": "由电气自动化设备", "801732": "电源设备", "801733": "高低压设备", "801734": "电机", "801735": "风电设备", "801736": "光伏设备", "801737": "电池",
    "801741": "航天装备", "801742": "航空装备", "801743": "地面兵装", "801744": "航海装备", "801745": "军工电子",
    "801751": "计算机设备", "801752": "IT服务", "801753": "软件开发",
    "801761": "出版", "801762": "广播电视", "801763": "数字媒体", "801764": "影视院线", "801765": "广告营销", "801766": "游戏",
    "801771": "通信设备", "801772": "通信服务",
    "801881": "乘用车", "801882": "商用车", "801883": "汽车零部件", "801884": "汽车服务", "801885": "摩托车",
    "801891": "通用设备", "801892": "专用设备", "801893": "仪器仪表", "801894": "金属制品", "801895": "工程机械", "801896": "轨交设备", "801897": "自动化设备",
    "801951": "黑色金属", "801952": "普钢", "801953": "特钢", "801961": "工业金属", "801962": "贵金属", "801963": "小金属", "801964": "金属新材料", "801965": "能源金属",
    "801971": "商业物业", "801972": "专业服务", "801981": "一般零售", "801982": "专业连锁", "801983": "电商及服务"
}

# ==========================================
# 诊断模块：检查环境
# ==========================================
def run_diagnostics():
    print(">>> 正在进行环境诊断...")
    print(f"Python Version: {sys.version}")
    print(f"AkShare Version: {ak.__version__}")
    
    # 检查 IP 归属地 (查看是否被识别为云厂商 IP)
    try:
        ip_info = requests.get("http://httpbin.org/ip", timeout=5).json()
        print(f"Current IP: {ip_info.get('origin', 'Unknown')}")
    except Exception as e:
        print(f"IP Check Failed: {e}")
    print("-" * 30)

# ==========================================
# 策略模块：定义多种数据获取方式
# ==========================================

# 策略 A: 乐咕乐股接口 (ak.sw_index_daily_indicator)
def strategy_legule(code, start_date, end_date):
    df = ak.sw_index_daily_indicator(
        symbol=code, 
        start_date="20200101", 
        end_date=end_date.strftime("%Y%m%d"), 
        data_type="Day"
    )
    if df is None or df.empty: return None
    # 统一列名
    df.rename(columns={'date': '日期', 'close': '收盘'}, inplace=True)
    return df

# 策略 B: 估值网接口 (ak.index_value_hist_funddb)
def strategy_funddb(code, start_date, end_date):
    df = ak.index_value_hist_funddb(symbol=code, indicator="收盘")
    if df is None or df.empty: return None
    df.rename(columns={'日期': '日期', '收盘': '收盘'}, inplace=True)
    return df

# 策略 C: 新浪接口 (ak.index_hist_sw) - 最容易被封，放最后
def strategy_sina(code, start_date, end_date):
    df = ak.index_hist_sw(symbol=code, period="day")
    if df is None or df.empty: return None
    # 默认列名通常就是 日期, 收盘
    return df

# 注册所有策略
STRATEGIES = [
    ("Legule (乐咕)", strategy_legule),
    ("FundDB (估值网)", strategy_funddb),
    ("Sina (新浪)", strategy_sina)
]

# ==========================================
# 主逻辑
# ==========================================
def fetch_and_save_data():
    run_diagnostics()
    
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730)
    
    # 1. 选拔最强策略
    # 我们拿 "801771" (通信设备) 作为测试用例，看哪个接口能通
    print(">>> 正在选拔最佳数据接口...")
    best_strategy = None
    best_strategy_name = ""
    
    test_code = "801771" # 通信设备
    
    for name, func in STRATEGIES:
        print(f"测试接口: {name} ...", end="")
        try:
            df = func(test_code, start_date, end_date)
            if df is not None and not df.empty and '收盘' in df.columns:
                print(" [成功 ✅]")
                best_strategy = func
                best_strategy_name = name
                break # 找到一个能用的就停止测试
            else:
                print(" [失败 ❌] 返回空数据")
        except Exception as e:
            print(f" [出错 ❌] {str(e)}")
            # 打印详细错误，方便你在 Actions 日志里看
            # traceback.print_exc() 

    if not best_strategy:
        print("\n!!! 严重错误: 所有申万接口均测试失败 !!!")
        print("建议: 可能 IP 被封锁，建议稍后重试，或改回东财板块数据。")
        # 这里不抛出异常，为了让 Actions 显示日志，我们生成一个空的 JSON 或者做个标记
        # 但为了通知你，还是抛出异常比较好
        raise RuntimeError("All AkShare SW interfaces failed.")

    print(f"\n>>> 选用策略: {best_strategy_name}")
    print(">>> 开始批量获取数据...")

    all_data = {}
    common_dates = []
    success_count = 0
    fail_count = 0

    # 2. 批量执行
    for code, name in SW_L2_DICT.items():
        try:
            # 使用选拔出来的最佳策略
            df = best_strategy(code, start_date, end_date)
            
            if df is None or df.empty:
                print(f"  [空] {name}")
                fail_count += 1
                continue

            # 统一数据清洗
            df['日期'] = pd.to_datetime(df['日期'])
            mask = (df['日期'] >= start_date) & (df['日期'] <= end_date)
            df_filtered = df.loc[mask].copy()
            df_filtered.sort_values('日期', inplace=True)
            
            dates = df_filtered['日期'].dt.strftime('%Y-%m-%d').tolist()
            closes = df_filtered['收盘'].tolist()
            
            if len(dates) == 0:
                fail_count += 1
                continue

            if not common_dates:
                common_dates = dates
            
            # 对齐逻辑
            if abs(len(dates) - len(common_dates)) < 15:
                # 简单裁切到相同长度 (取后部)
                min_len = min(len(dates), len(common_dates))
                all_data[name] = closes[-min_len:]
                if len(common_dates) > min_len:
                    common_dates = common_dates[-min_len:]
                success_count += 1
            else:
                # 如果差异太大，且这是前几个数据，尝试重置基准
                if success_count < 3:
                    common_dates = dates
                    all_data[name] = closes
                    success_count += 1
                else:
                    print(f"  [丢弃] {name}: 长度差异大 ({len(dates)} vs {len(common_dates)})")
                    fail_count += 1

            # 动态延时：防止请求过快触发风控
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            print(f"  [错] {name}: {str(e)[:50]}")
            fail_count += 1
            continue

    print(f"\n>>> 统计: 成功 {success_count}, 失败 {fail_count}")

    if success_count > 0:
        final_json = {
            "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dates": common_dates,
            "data": all_data
        }
        with open("industry_data.json", "w", encoding="utf-8") as f:
            json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
        print(">>> 文件保存成功！")
    else:
        raise RuntimeError("没有获取到任何有效数据")

if __name__ == "__main__":
    fetch_and_save_data()
