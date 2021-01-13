#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
使用akshare库导入股票历史数据

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""
import os
import pandas as pd

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
            
    return stocklist

def download_stockdata():
    for i in stocklist:
        if i[0:1] == '6':
            ii = 'sh' + i
        elif i[0:1] == '0' or i[0:1] == '3':
            ii = 'sz' + i
        
        process_info = '[' + str(stocklist.index(i) + 1) + '/' + str(len(stocklist)) + '] ' + i
        try:
            df = akshare.stock_zh_a_daily(symbol=ii, start_date="19901219", end_date="20210113", adjust="qfq")
            csv_path = ucfg.csv_path + os.sep + i + '.csv'
            df.to_csv(csv_path)
        except:
            print(process_info + ' >>>wrong<<<')
        else:
            print(process_info + ' done')
    
    print('处理完毕')

# 主程序开始
            
# 下载最新股票代码列表            
stocklist = download_stocklist()

#定义要下载的股票区间
start_stock_num = ''  # 留空则从头开始处理 不需要输入sh/sz
end_stock_num = ''  # 留空则处理到末尾
stocklist = update_stocklist(stocklist, start_stock_num, end_stock_num)

download_stockdata()