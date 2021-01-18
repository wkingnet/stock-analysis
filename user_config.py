#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
用户设置文件

作者：wking [http://wkings.net]
"""

#配置部分开始
debug = 1   #是否开启调试日志输出  1开 0关
tdx_path = 'd:/stock/通达信'   #指定通达信目录
csv_path = 'd:/日线数据'  #指定数据保存目录
csv_index = 'd:/日线数据/指数'  #指定数据保存目录
dividend_dir = 'd:/除权除息数据'  #指定除权除息保存目录
index_list = [  # 需要转换的指数文件。通达信按998查看重要指数
    'sh999999.day',  # 上证指数
    'sh000300.day',  # 沪深300
    'sz399001.day',  # 深成指
]

#配置部分结束