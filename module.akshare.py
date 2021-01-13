#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
使用akshare库导入股票历史数据

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""
import pandas as pd
import csv

import akshare as ak

import user_config as ucfg

def 
# 获取沪深 A 股股票代码和简称数据
stock_list = ak.stock_info_a_code_name()
df = pd.DataFrame(stock_list)
df.to_csv(ucfg.csv_path + '/123.csv')

f_obj = open(ucfg.csv_path + '/123.csv', 'r', encoding="utf-8")  # 以覆盖写模式打开文件
f_obj.close()


print(stocklist)
