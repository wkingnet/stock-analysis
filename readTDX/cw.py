#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
读取通达信专业财务数据文件 /vipdoc/cw/gpcw?????.dat
感谢大神们的研究 https://github.com/rainx/pytdx/issues/133

财务文件更新方法：打开通达信软件，工具栏-系统-专业财务数据，通达信会自动提示是否有需要更新的财务数据包。
财务文件无需天天更新，上市公司发了季报后财务文件才会更新，因此更新大概率集中在财报季。

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""

import os
import csv
import time
import datetime
import pandas as pd

import func_TDX
import user_config as ucfg


# 变量定义
tdxpath = ucfg.tdx['tdx_path']

starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()


# 主程序开始
# TODO 财报文件使用代码自动更新下载功能
# 读取财务文件
cw_path = tdxpath + os.sep + "vipdoc" + os.sep + "cw"
tmplist = os.listdir(cw_path)  # 遍历通达信vipdoc/cw目录
cw_filelist = []
for file in tmplist:  # 只保留gpcw????????.dat格式文件
    if len(file) == 16 and file[:4] == "gpcw" and file[-4:] == ".dat":
        cw_filelist.append(file)
print(f'检测到{len(cw_filelist)}个专业财务文件')

# 解析财务文件
for file in cw_filelist:
    process_info = f'[{(cw_filelist.index(file) + 1):>3}/{str(len(cw_filelist))}] {file}'
    filepath = cw_path + os.sep + file
    df = func_TDX.historyfinancialreader(filepath)
    csvpath = ucfg.tdx['csv_cw'] + os.sep + file[:-4] + ".csv"
    df.to_csv(csvpath, encoding='gbk', index=True, header=False)
    print(f'{process_info} 完成 已用{(time.time() - starttime_tick):>5.2f}秒')
