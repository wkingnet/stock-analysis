#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
用户设置文件

作者：wking [http://wkings.net]
"""

# 配置部分开始
debug = 1  # 是否开启调试日志输出  1开 0关

# 目录需要事先手动建立好，不然程序会出错
tdx = {
    'tdx_path': 'd:/stock/通达信/',  # 指定通达信目录
    'csv_day': 'd:/通达信数据/lday_qfq/',  # 指定数据保存目录
    'csv_index': 'd:/通达信数据/index/',  # 指定指数保存目录
    'csv_cw': 'd:/通达信数据/cw/',  # 指定专业财务保存目录
}

index_list = [  # 通达信需要转换的指数文件。通达信按998查看重要指数
    'sh999999.day',  # 上证指数
    'sh000300.day',  # 沪深300
    'sz399001.day',  # 深成指
]


# 用不到的参数
"""
baostock = {
    'csv_day_bfq': 'd:/baostock/日线数据bfq',  # 指定日线数据不复权保存目录
    'csv_day_qfq': 'd:/日线数据qfq',  # 指定数据前复权保存目录
    'csv_index': 'd:/baostock/指数',  # 指定指数保存目录
    'dividend_dir': 'd:/baostock/除权除息数据',  # 指定除权除息保存目录
    'adjust_factor_dir': 'd:/baostock/复权因子数据',  # 指定复权因子保存目录
    'index_list': ['sh.000001', 'sh.000300', 'sz.399001']  # baostock要下载的指数。000001上证指数，000300沪深300，399001深成指
}
"""
# 配置部分结束
