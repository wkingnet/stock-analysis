#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
使用baostock库导入股票历史数据

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""

import os
import time
import pandas as pd

import baostock
import akshare

import user_config as ucfg


# 获取沪深 A 股股票代码和简称数据
def download_stocklist():
    """
    调用akshare库获取当前最新A股股票列表，返回列表类型的代码
    """
    df = pd.DataFrame(akshare.stock_info_a_code_name())
    stocklist = df['code'].tolist()
    return stocklist

# 主程序开始
starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()

#### 登陆系统 ####
lg = baostock.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# 下载最新股票代码列表
stocklist = download_stocklist()

for i in stocklist:
    if i[0:1] == '6':
        ii = 'sh.' + i
    elif i[0:1] == '0' or i[0:1] == '3':
        ii = 'sz.' + i

    process_info = '[' + str(stocklist.index(i) + 1) + '/' + str(len(stocklist)) + '] ' + i
    try:
        rs = baostock.query_history_k_data_plus(ii,
            "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pbMRQ,isST",
            start_date='1991-01-01', end_date='',
            frequency="d", adjustflag="3")
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
        print(process_info + ' 完成 开始时间[' + starttime_str + '] 已用'
              + str(round(time.time() - starttime_tick)) + '秒')
        csv_file = ucfg.csv_path + os.sep + i + '.csv'
        result.to_csv(csv_file, index=True)

#### 登出系统 ####
baostock.logout()