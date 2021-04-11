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
import sys
import time
import pandas as pd
import argparse
from tqdm import tqdm
from multiprocessing import Pool, RLock, freeze_support
import func
import user_config as ucfg


def check_files_exist():
    # 判断目录和文件是否存在，存在则直接删除
    if os.path.exists(ucfg.tdx['csv_lday']) or os.path.exists(ucfg.tdx['csv_index']):
        # choose = input("文件已存在，输入 y 删除现有文件并重新生成完整数据，其他输入则附加最新日期数据: ")
        if 'del' in str(sys.argv[1:]):
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
            for root, dirs, files in os.walk(ucfg.tdx['pickle'], topdown=False):
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
            except FileExistsError:
                pass
            try:
                os.mkdir(ucfg.tdx['pickle'])
            except FileExistsError:
                pass
    else:
        os.mkdir(ucfg.tdx['csv_lday'])
        os.mkdir(ucfg.tdx['csv_index'])


def update_lday():
    # 读取通达信正常交易状态的股票列表。infoharbor_spec.cfg退市文件不齐全，放弃使用
    tdx_stocks = pd.read_csv(ucfg.tdx['tdx_path'] + '/T0002/hq_cache/infoharbor_ex.code',
                             sep='|', header=None, index_col=None, encoding='gbk', dtype={0: str})
    file_listsh = tdx_stocks[0][tdx_stocks[0].apply(lambda x: x[0:1] == "6")]
    file_listsz = tdx_stocks[0][tdx_stocks[0].apply(lambda x: x[0:1] != "6")]

    print("处理深市股票")
    # file_list = os.listdir(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday')
    for f in tqdm(file_listsz):
        f = 'sz' + f + '.day'
        if os.path.exists(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday/' + f):  # 处理深市sh00开头和创业板sh30文件，否则跳过此次循环
            # print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
            func.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday', f, ucfg.tdx['csv_lday'])

    print("处理沪市股票")
    # file_list = os.listdir(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday')
    for f in tqdm(file_listsh):
        # 处理沪市sh6开头文件，否则跳过此次循环
        f = 'sh' + f + '.day'
        if os.path.exists(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday/' + f):
            # print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
            func.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday', f, ucfg.tdx['csv_lday'])

    print("处理指数文件")
    for i in tqdm(ucfg.index_list):
        # print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + i)
        if 'sh' in i:
            func.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday', i, ucfg.tdx['csv_index'])
        elif 'sz' in i:
            func.day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday', i, ucfg.tdx['csv_index'])


def qfq(file_list, df_gbbq, cw_dict, tqdm_position=None):
    tq = tqdm(file_list, leave=False, position=tqdm_position)
    for filename in tq:
        # process_info = f'[{(file_list.index(filename) + 1):>4}/{str(len(file_list))}] {filename}'
        df_bfq = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + filename, index_col=None, encoding='gbk',
                             dtype={'code': str})
        df_qfq = func.make_fq(filename[:-4], df_bfq, df_gbbq, cw_dict)
        # lefttime_tick = int((time.time() - starttime_tick) / (file_list.index(filename) + 1) * (len(file_list) - (file_list.index(filename) + 1)))
        if len(df_qfq) > 0:  # 返回值大于0，表示有更新
            df_qfq.to_csv(ucfg.tdx['csv_lday'] + os.sep + filename, index=False, encoding='gbk')
            df_qfq.to_pickle(ucfg.tdx['pickle'] + os.sep + filename[:-4] + '.pkl')
            tq.set_description(filename + "复权完成")
        #     print(f'{process_info} 复权完成 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')
        else:
            tq.set_description(filename + "无需更新")
        #     print(f'{process_info} 无需更新 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')


if __name__ == '__main__':
    print('附带命令行参数 readTDX_lday.py del 删除现有文件并重新生成完整数据')
    # print('参数列表:', str(sys.argv[1:]))
    # print('脚本名:', str(sys.argv[0]))

    # 主程序开始
    check_files_exist()
    update_lday()
    # 通达信文件处理完成

    # 处理生成的通达信日线数据，复权加工代码
    file_list = os.listdir(ucfg.tdx['csv_lday'])
    starttime_tick = time.time()
    df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})
    cw_dict = func.readall_local_cwfile()

    # 多进程
    # print('Parent process %s' % os.getpid())
    t_num = os.cpu_count()-2  # 进程数 读取CPU逻辑处理器个数
    div, mod = int(len(file_list) / t_num), len(file_list) % t_num
    freeze_support()  # for Windows support
    tqdm.set_lock(RLock())  # for managing output contention
    p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
    for i in range(0, t_num):
        if i + 1 != t_num:
            # print(i, i * div, (i + 1) * div)
            p.apply_async(qfq, args=(file_list[i * div:(i + 1) * div], df_gbbq, cw_dict, i))
        else:
            # print(i, i * div, (i + 1) * div + mod)
            p.apply_async(qfq, args=(file_list[i * div:(i + 1) * div + mod], df_gbbq, cw_dict, i))
    p.close()
    p.join()

    print('日线数据全部处理完成')
