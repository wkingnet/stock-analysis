"""
选股文件。导入数据——执行策略——显示结果
为保证和通达信选股一致，需使用前复权数据
"""
import os
import csv
import time
import datetime
import pandas as pd

import CeLue
import user_config as ucfg


# 变量定义
tdxpath = ucfg.tdx['tdx_path']
csvdaypath = ucfg.baostock['csv_day_qfq']
stockcelue = []  # 策略选出的股票

starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()


# 主程序开始
# 读取股票列表
stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_day'])]  # 去文件名里的.csv，生成纯股票代码list
for stockcode in stocklist:
    process_info = f'[{(stocklist.index(stockcode) + 1):>4}/{str(len(stocklist))}] {stockcode}'
    csvfile = csvdaypath + os.sep + stockcode + '.csv'
    df = pd.read_csv(csvfile, encoding='gbk', index_col=0)
    df = df.set_index('date')
    cl1 = CeLue.策略1(df)
    if cl1:
        stockcelue.append(stockcode)
    print(f'{process_info} 完成，已选出{len(stockcelue)}只股票 已用{(time.time() - starttime_tick):.2f}秒 剩余预计'
    f'{int((time.time()-starttime_tick)/(stocklist.index(stockcode)+1)*(len(stocklist)-(stocklist.index(stockcode)+1)))}秒')
print(f'全部完成，已选出{len(stockcelue)}只股票，清单:')
print(stockcelue)
