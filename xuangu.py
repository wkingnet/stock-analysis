"""
选股文件。导入数据——执行策略——显示结果
为保证和通达信选股一致，需使用前复权数据
"""
import os
import csv
import time
import datetime
import pandas as pd

from celue import *
import user_config as ucfg


# 变量定义
tdxpath = ucfg.tdx['tdx_path']
csvdaypath = ucfg.baostock['csv_day_qfq']
stockcelue = []

# 主程序开始
# 读取股票列表
stockcodelist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_day'])]  # 去文件名里的.csv，生成纯股票代码list
for stockcode in stockcodelist:
    csvfile = csvdaypath + os.sep + stockcode + '.csv'
    df = pd.read_csv(csvfile, encoding='gbk', index_col=0)
    df = df.set_index('date')
    cl1 = 策略1(df)
    if cl1:
        stockcelue.append(stockcode)
print(f'已选出的股票 {print(stockcelue)}')