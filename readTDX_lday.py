#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
在网络读取通达信代码上修改加工，增加、完善了一些功能
1、增加了读取深市股票功能。
2、增加了在已有数据的基础上追加最新数据，而非完全删除重灌。
3、增加了读取上证指数、沪深300指数功能。
4、没有使用pandas库，但输出的CSV格式是pandas库的dataFrame格式。
5、过滤了无关的债券指数、板块指数等，只读取沪市、深市A股股票。

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""
import os
import time
import pandas as pd

import func_TDX
import user_config as ucfg

# 变量初始化
used_time = {}  # 创建一个计时字典

# 主程序开始
# 判断目录和文件是否存在，存在则直接删除
if os.path.exists(ucfg.tdx['csv_lday']) or os.path.exists(ucfg.tdx['csv_index']):
    choose = input("文件已存在，输入 y 删除现有文件并重新生成完整数据，其他输入则附加最新日期数据: ")
    if choose == 'y':
        for root, dirs, files in os.walk(ucfg.tdx['csv_lday'], topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        for root, dirs, files in os.walk(ucfg.tdx['csv_index'], topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        try:
            os.mkdir(ucfg.tdx['csv_lday'])
        except FileExistsError:
            pass
        try:
            os.mkdir(ucfg.tdx['csv_index'])
        except  FileExistsError:
            pass
else:
    os.mkdir(ucfg.tdx['csv_lday'])
    os.mkdir(ucfg.tdx['csv_index'])

# 读取退市股票列表
delisted_stocks = pd.read_csv(ucfg.tdx['tdx_path'] + '/T0002/hq_cache/infoharbor_spec.cfg',
                              sep='|', header=None, index_col=None, encoding='gbk', dtype={1: str})
delisted_stocks = delisted_stocks[1].tolist()

# 处理深市股票
file_list = os.listdir(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday')
[file_list.remove(i) for i in file_list[:] if i[2:-4] in delisted_stocks]  # 从列表里删除已退市股票
used_time['sz_begintime'] = time.time()
for f in file_list:
    if f[0:4] == 'sz00' or f[0:4] == 'sz30':  # 处理深市sh00开头和创业板sh30文件，否则跳过此次循环
        print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
        func_TDX.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday', f, ucfg.tdx['csv_lday'])
used_time['sz_endtime'] = time.time()

# 处理沪市股票
file_list = os.listdir(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday')
[file_list.remove(i) for i in file_list[:] if i[2:-4] in delisted_stocks]  # 从列表里删除已退市股票
used_time['sh_begintime'] = time.time()
for f in file_list:
    # 处理沪市sh6开头文件，否则跳过此次循环
    if f[0:3] == 'sh6':
        print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
        func_TDX.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday', f, ucfg.tdx['csv_lday'])
used_time['sh_endtime'] = time.time()

# 处理指数文件
used_time['index_begintime'] = time.time()

for i in ucfg.index_list:
    print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + i)
    if 'sh' in i:
        func_TDX.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday', i, ucfg.tdx['csv_index'])
    elif 'sz' in i:
        func_TDX.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday', i, ucfg.tdx['csv_index'])

used_time['index_endtime'] = time.time()

print('沪市处理完毕，用时' + str(int(used_time['sh_endtime'] - used_time['sh_begintime'])) + '秒')
print('深市处理完毕，用时' + str(int(used_time['sz_endtime'] - used_time['sz_begintime'])) + '秒')
print('指数文件处理完毕，用时' + str(int(used_time['index_endtime'] - used_time['index_begintime'])) + '秒')
# 通达信文件处理完成

# 处理生成的通达信日线数据，复权加工 部分代码
file_list = os.listdir(ucfg.tdx['csv_lday'])
starttime_tick = time.time()
df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})
cw_dict = func_TDX.readall_local_cwfile()
for filename in file_list:
    process_info = f'[{(file_list.index(filename) + 1):>4}/{str(len(file_list))}] {filename}'
    df_bfq = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + filename, index_col=None, encoding='gbk', dtype={'code': str})
    df_qfq = func_TDX.make_fq(filename[:-4], df_bfq, df_gbbq, cw_dict)
    lefttime_tick = int((time.time() - starttime_tick) / (file_list.index(filename) + 1)
                        * (len(file_list) - (file_list.index(filename) + 1)))
    if len(df_qfq) > 0:  # 返回值大于0，表示有更新
        df_qfq.to_csv(ucfg.tdx['csv_lday'] + os.sep + filename, index=False, encoding='gbk')
        df_qfq.to_pickle(ucfg.tdx['pickle'] + os.sep + filename[:-4] + '.pkl')
        print(f'{process_info} 复权完成 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')
    else:
        print(f'{process_info} 无需更新 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')
print('日线数据全部处理完成')
