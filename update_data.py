import akshare as ak
import pandas as pd
import json
import datetime
import time
import sys

# ==========================================
# 为了防止 GitHub 服务器下载列表失败
# 我们直接内置申万二级行业代码表 (2021版标准)
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

def fetch_and_save_data():
    print(f"Step 1: 准备获取申万二级行业数据，共 {len(SW_L2_DICT)} 个目标...")
    
    # 设定时间范围
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730) 
    start_str = "20200101" # 接口请求开始时间，取长一点
    end_str = end_date.strftime("%Y%m%d")
    
    all_data = {}
    common_dates = []
    success_count = 0
    fail_count = 0
    
    # 循环获取数据
    for code, name in SW_L2_DICT.items():
        try:
            # 使用 ak.sw_index_daily_indicator 接口 (数据源: Legule)
            # 这个接口在 GitHub Actions 环境下最稳定
            # print(f"正在获取: {name} ({code})...")
            
            df = ak.sw_index_daily_indicator(symbol=code, start_date=start_str, end_date=end_str, data_type="Day")
            
            if df is None or df.empty:
                print(f"  [跳过] {name}: 返回空数据")
                fail_count += 1
                continue

            # 数据清洗
            # Legule 返回的 date 是 object 或 datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # 筛选我们需要的日期范围 (最近2年)
            mask = (df['date'] >= start_date) & (df['date'] <= end_date)
            df_filtered = df.loc[mask].copy()
            
            # 格式化日期
            dates = df_filtered['date'].dt.strftime('%Y-%m-%d').tolist()
            # 获取收盘价
            closes = df_filtered['close'].tolist()
            
            if len(dates) == 0:
                fail_count += 1
                continue

            # 以第一个成功的数据建立日期基准
            if not common_dates:
                common_dates = dates
            
            # 简单对齐：只收录长度一致的数据，防止前端画图错位
            # 实际生产中指数的交易日基本一致，差异极小
            if len(dates) == len(common_dates):
                all_data[name] = closes
                success_count += 1
            else:
                # 如果长度差异极小（比如差1天），可以考虑前端容错，但为了稳定性先跳过
                if abs(len(dates) - len(common_dates)) < 5:
                    # 尝试裁切对齐（取最后N天）
                    min_len = min(len(dates), len(common_dates))
                    all_data[name] = closes[-min_len:]
                    if len(common_dates) > min_len:
                        common_dates = common_dates[-min_len:] # 更新基准为较短的
                    success_count += 1
                else:
                    print(f"  [跳过] {name}: 长度不一致 ({len(dates)} vs {len(common_dates)})")
                    fail_count += 1

            # 极短延时，防止请求过快
            time.sleep(0.05)

        except Exception as e:
            print(f"  [错误] {name} ({code}): {e}")
            fail_count += 1
            continue

    print(f"\nStep 2: 获取结束。成功: {success_count}, 失败/跳过: {fail_count}")

    if success_count == 0:
        # 如果全部失败，抛出错误让 Action 变红
        raise ValueError("严重错误：没有获取到任何有效数据！")

    # 构建最终 JSON
    final_json = {
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dates": common_dates,
        "data": all_data
    }

    # 保存
    with open("industry_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"Step 3: industry_data.json 保存成功，包含 {len(all_data)} 个行业。")

if __name__ == "__main__":
    fetch_and_save_data()
