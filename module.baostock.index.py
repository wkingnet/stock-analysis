#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
使用baostock库导入股票历史数据

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""

import os
import time
import datetime
import pandas as pd

import baostock

import user_config as ucfg

#变量定义
starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()

# 获取沪深 A 股股票代码和简称数据
def download_stocklist():
    """
    调用baostock库获取当前最新A股股票列表，返回列表类型的代码
    """
    data_list = []
    stocklist = []

    # 方法说明：获取指定交易日期所有股票列表。通过API接口获取证券代码及股票交易状态信息，与日K线数据同时更新。
    # 可以通过参数‘某交易日’获取数据（包括：A股、指数），提供2006 - 今数据。
    # 返回类型：pandas的DataFrame类型。
    # 由于当天股票列表需等16点以后才生成，因此用昨天的股票列表。
    rs = baostock.query_all_stock(day=(datetime.date.today() + datetime.timedelta(-1)))

    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    # print(data_list)
    for data in data_list:  # 筛选获得的数据列表，只取股票列表，剔除指数
        if data[0][:4] == 'sh.6' or data[0][:5] == 'sz.00' or data[0][:5] == 'sz.30':
            stocklist.append(data[0][3:])
    print(f'股票列表获取完成，共有{len(stocklist)}只股票')
    return stocklist

# 切片股票列表stocklist，指定开始股票和结束股票
def update_stocklist(stocklist, start_num, end_num):
    """


    Parameters
    ----------
    start_num : str
    从哪只股票开始处理

    end_num : str
    处理到哪只股票为止

    Returns
    -------
    处理后的股票代码列表

    """
    for i in stocklist:
        if i == start_num:
            start_index = stocklist.index(i)
            stocklist = stocklist[start_index:]
        if i == end_num:
            end_index = stocklist.index(i)
            stocklist = stocklist[:end_index]

    print(f'股票列表切片完成，共有{len(stocklist)}只股票')
    return stocklist


# 主程序开始

# 判断目录和文件是否存在，存在则直接删除
if os.path.exists(ucfg.baostock['csv_index']):
    choose = input("文件已存在，输入 y 删除现有文件并重新生成完整数据，其他输入则附加最新日期数据: ")
    if choose == 'y':
        for root, dirs, files in os.walk(ucfg.baostock['csv_index'], topdown=False):
            for name in files:
                os.remove(os.path.join(root,name))
            for name in dirs:
                os.rmdir(os.path.join(root,name))
        try:
            os.mkdir(ucfg.baostock['csv_index'])
        except FileExistsError:
            pass
else:
    os.mkdir(ucfg.baostock['csv_index'])

#### 登陆系统 ####
lg = baostock.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

for i in ucfg.baostock['index_list']:
    process_info = '[' + str(ucfg.baostock['index_list'].index(i) + 1) + '/' + str(len(ucfg.baostock['index_list'])) + '] ' + i
    try:
        rs = baostock.query_history_k_data_plus(i,
            "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg",
            start_date='1990-12-19', end_date='',
            frequency="d")
    except:
        print(process_info + ' >>>wrong<<<')
        print('query_history_k_data_plus respond error_code:' + rs.error_code)
        print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
    else:
        #### 打印结果集 ####
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        print(f'{process_info} 完成 已用{str(round(time.time() - starttime_tick, 2))}秒 开始时间[{starttime_str}]')
        csv_file = ucfg.baostock['csv_index'] + os.sep + i + '.csv'
        result.to_csv(csv_file, index=True)

#### 登出系统 ####
baostock.logout()

# 给上证指数文件加上【全部A股股价平均数值列】
df_index = pd.read_csv(ucfg.baostock['csv_index'] + '/sh.000001.csv', usecols=['date'])  # 读取上证指数文件，只读取日期列
file_list = os.listdir(ucfg.baostock['csv_day'])  # 日线数据列表

index_row_num = 0
while index_row_num < df_index.shape[0]:  # 循环df_index全部行，也就是A股全部交易天数
    day_avg = 0  # 当天所有股票开高低收平均值相加的平均值
    day_trade_num = 0  # 当天交易的股票的数量
    for file in file_list:  # 所有股票循环一次
        index_row_date = df_index.iat[index_row_num, 0]  # 读取指数文件当前行保存的日期
        # 读取file，提取数据。以日期为索引
        df_csvday = pd.read_csv(ucfg.baostock['csv_day'] + os.sep + file, index_col='date',
                                usecols=['date', 'open', 'high', 'low', 'close', 'tradestatus'])
        if index_row_date in df_csvday.index:  # 如果当前指数日期在CSVDAY的索引列表里，表示此股票当日有数据
            if df_csvday.at[index_row_date, 'tradestatus'] == 1:  # tradestatus==1表示正常交易状态
                day_trade_num = day_trade_num + 1
                open = df_csvday.at[index_row_date, 'open']
                high = df_csvday.at[index_row_date, 'high']
                low = df_csvday.at[index_row_date, 'low']
                close = df_csvday.at[index_row_date, 'close']
                stock_avg = (open + high + low + close) / 4  #当日该股票开高低收的平均值，保留2位小数
                day_avg = day_avg + stock_avg
    if day_avg != 0 and day_trade_num != 0:
        day_avg = round(day_avg / day_trade_num, 2)  # 计算当天所有股票的平均值
    print(f'[{index_row_num + 1}/{df_index.shape[0]}] {index_row_date}')