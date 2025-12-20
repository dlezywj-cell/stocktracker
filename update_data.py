import requests
import pandas as pd
import json
import datetime
import time
import random

# ==========================================
# 申万二级行业代码表 (2021版)
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
    "801731": "电气自动化设备", "801732": "电源设备", "801733": "高低压设备", "801734": "电机", "801735": "风电设备", "801736": "光伏设备", "801737": "电池",
    "801741": "航天装备", "801742": "航空装备", "801743": "地面兵装", "801744": "航海装备", "801745": "军工电子",
    "801751": "计算机设备", "801752": "IT服务", "801753": "软件开发",
    "801761": "出版", "801762": "广播电视", "801763": "数字媒体", "801764": "影视院线", "801765": "广告营销", "801766": "游戏",
    "801771": "通信设备", "801772": "通信服务",
    "801881": "乘用车", "801882": "商用车", "801883": "汽车零部件", "801884": "汽车服务", "801885": "摩托车",
    "801891": "通用设备", "801892": "专用设备", "801893": "仪器仪表", "801894": "金属制品", "801895": "工程机械", "801896": "轨交设备", "801897": "自动化设备",
    "801951": "黑色金属", "801952": "普钢", "801953": "特钢", "801961": "工业金属", "801962": "贵金属", "801963": "小金属", "801964": "金属新材料", "801965": "能源金属",
    "801971": "商业物业", "801972": "专业服务", "801981": "一般零售", "801982": "专业连锁", "801983": "电商及服务"
}

# 缓存 SecID，避免重复搜索
SECID_CACHE = {}

def get_real_secid(code):
    """
    通过东财搜索接口，获取准确的市场代码 (secid)
    """
    if code in SECID_CACHE:
        return SECID_CACHE[code]
        
    url = "https://searchapi.eastmoney.com/api/suggest/get"
    params = {
        "input": code,
        "type": "14", # 14代表指数/板块搜索
        "token": "D43BF722C8E33BDC906FB84D85E326E8"
    }
    try:
        res = requests.get(url, params=params, timeout=3)
        data = res.json()
        if "QuotationCodeTable" in data and "Data" in data["QuotationCodeTable"]:
            items = data["QuotationCodeTable"]["Data"]
            for item in items:
                # 找到完全匹配的代码
                if item.get("Code") == code:
                    quote_id = item.get("QuoteID") # 如 "2.801771"
                    if quote_id:
                        SECID_CACHE[code] = quote_id
                        return quote_id
    except Exception as e:
        print(f"  [搜索失败] {code}: {e}")
        pass
    
    # 如果搜索失败，默认尝试 "2." (申万指数通常在市场2)
    return f"2.{code}"

def get_em_kline(sw_code, start_date_str, end_date_str):
    """
    获取K线
    """
    # 1. 智能获取 SecID
    secid = get_real_secid(sw_code)
    
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101", # 日线
        "fqt": "1",   # 前复权
        "beg": start_date_str,
        "end": end_date_str,
        "smplmt": "1000"
    }
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        res = requests.get(url, params=params, headers=headers, timeout=5)
        data = res.json()
        
        if data and data.get("data") and data["data"].get("klines"):
            klines = data["data"]["klines"]
            dates = []
            closes = []
            for line in klines:
                parts = line.split(",")
                dates.append(parts[0])
                closes.append(float(parts[2]))
            return dates, closes
        else:
            # 如果没数据，可能是 SecID 错了，打印出来方便调试
            # print(f"  [API无返回] {sw_code} using secid={secid}")
            pass
            
    except Exception:
        pass
            
    return None, None

def fetch_and_save_data():
    print(f"Step 1: 启动智能抓取，目标 {len(SW_L2_DICT)} 个申万行业...")
    
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730)
    
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    
    all_data = {}
    common_dates = []
    success_count = 0
    fail_count = 0
    
    for code, name in SW_L2_DICT.items():
        try:
            dates, closes = get_em_kline(code, start_str, end_str)
            
            if not dates:
                print(f"  [失败] {name} ({code}) - 无数据")
                fail_count += 1
                continue
                
            if not common_dates:
                common_dates = dates
            
            # 对齐逻辑
            if abs(len(dates) - len(common_dates)) < 15:
                min_len = min(len(dates), len(common_dates))
                all_data[name] = closes[-min_len:]
                if len(common_dates) > min_len:
                    common_dates = common_dates[-min_len:]
                success_count += 1
            else:
                # 如果是前几个，重置基准
                if success_count < 3:
                    common_dates = dates
                    all_data[name] = closes
                    success_count += 1
                else:
                    print(f"  [丢弃] {name}: 长度不一致 {len(dates)} vs {len(common_dates)}")
                    fail_count += 1
            
            # 成功获取，打印一个小点表示进度
            # print(".", end="", flush=True)
            time.sleep(0.05)
            
        except Exception as e:
            print(f"  [错误] {name}: {e}")
            fail_count += 1

    print(f"\n\nStep 2: 统计 - 成功: {success_count}, 失败: {fail_count}")

    if success_count > 0:
        final_json = {
            "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dates": common_dates,
            "data": all_data
        }
        with open("industry_data.json", "w", encoding="utf-8") as f:
            json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
        print("SUCCESS: 申万数据抓取完成！")
    else:
        raise ValueError("严重错误：未能获取任何数据")

if __name__ == "__main__":
    fetch_and_save_data()
