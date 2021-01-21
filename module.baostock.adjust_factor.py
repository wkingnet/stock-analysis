#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
使用baostock库的API接口获取复权因子信息数据
baostock.com/baostock/index.php/除权除息信息
作者：wking [http://wkings.net]
"""

import os
import csv
import time
import datetime
import pandas as pd

import baostock

import user_config as ucfg

#变量定义部分
#定义要下载的股票区间
start_stock_num = ''  # 留空则从头开始处理 不需要输入sh/sz
end_stock_num = ''  # 留空则处理到末尾

starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()
thisyear = time.strftime("%Y", time.localtime())
today = time.strftime("%Y-%m-%d", time.localtime())


# 函数部分
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
    rs = baostock.query_all_stock(day=time.strftime("%Y-%m-%d", time.localtime()))

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

def stock_get_lastdate(stockcode):
    """
    获取输入的CSV文件的已有最新日期。返回最新日期。日期必须位于CSV文件的第二列
    """
    file = ucfg.baostock['adjust_factor_dir'] + os.sep + stockcode
    with open(file) as f_obj:
        csv_obj = csv.reader(f_obj)
        for row in csv_obj:  # 循环读取CSV的每一行，自动读取到末尾行，即可获取最新的日期。日期列必须位于第2列
            lastdate = row[1]
    if lastdate == 'dividOperateDate':  # 如果是dividOperateDate，表示没有除权因子，是新股。直接返回大日期跳过。
        lastdate = '2999-01-01'
    else:
        lastdate = datetime.datetime.strptime(lastdate, '%Y-%m-%d')
        delta = datetime.timedelta(days=1)
        lastdate = lastdate + delta  # 获取到的日期加1天，表示从下一天开始获取数据
        lastdate = lastdate.strftime('%Y-%m-%d')
    return lastdate


# 主程序开始
# 判断目录和文件是否存在，存在则直接删除
if os.path.exists(ucfg.baostock['adjust_factor_dir']):
    choose = input("文件已存在，输入 y 删除现有文件并重新生成完整数据，其他输入则附加最新日期数据: ")
    if choose == 'y':
        for root, dirs, files in os.walk(ucfg.baostock['adjust_factor_dir'], topdown=False):
            for name in files:
                os.remove(os.path.join(root,name))
            for name in dirs:
                os.rmdir(os.path.join(root,name))
        try:
            os.mkdir(ucfg.baostock['adjust_factor_dir'])
        except FileExistsError:
            pass
else:
    os.mkdir(ucfg.baostock['adjust_factor_dir'])

#### 登陆系统 ####
lg = baostock.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# 下载最新股票代码列表
stocklist = download_stocklist()
stocklist = update_stocklist(stocklist, start_stock_num, end_stock_num)

for i in stocklist:  # 循环股票列表stocklist
    if i[0:1] == '6':
        ii = 'sh.' + i
    elif i[0:1] == '0' or i[0:1] == '3':
        ii = 'sz.' + i

    process_info = f'[{(stocklist.index(i) + 1):>4}/{str(len(stocklist))}] {i}'
    csv_file = ucfg.baostock['adjust_factor_dir'] + os.sep + i + '.csv'
    if choose == 'y' or not os.path.exists(csv_file):
        start_date = '1990-12-19'  # 无已下载数据，指定股票下载起始日期，重头开始下载
    else:
        start_date = stock_get_lastdate(i + '.csv')  # 获取当前已下载股票CSV的最新日期
        if start_date > str(datetime.date.today()):  # 如果日期大于今天，跳过此次循环
            print(f'{process_info} 日期大于今天，无需更新，跳过')
            continue

    rs_list = []
    rs_factor = baostock.query_adjust_factor(code=ii, start_date=start_date, end_date=today)
    while (rs_factor.error_code == '0') & rs_factor.next():
        rs_list.append(rs_factor.get_row_data())
    result_factor = pd.DataFrame(rs_list, columns=rs_factor.fields)
    result_factor['code'] = i  # 将code列保存的字符串sh.600000样式股票代码，替换为整数型的600000

    if choose == 'y' or not os.path.exists(csv_file):
        result_factor.to_csv(csv_file, encoding="gbk", index=False)
    else:
        df = pd.read_csv(csv_file, index_col=0)
        df = df.append(result_factor, ignore_index=True)
        df.to_csv(csv_file, index=True)

    print(f'{process_info} 完成 从 {start_date} 起更新数据 已用{(time.time() - starttime_tick):.2f}秒 剩余预计'
          f'{int((time.time()-starttime_tick)/(stocklist.index(i)+1)*(len(stocklist)-(stocklist.index(i)+1)))}秒')

#### 登出系统 ####
baostock.logout()