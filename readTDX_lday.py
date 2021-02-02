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
from struct import unpack
from decimal import Decimal  # 用于浮点数四舍五入

import func_TDX
import user_config as ucfg

# 变量初始化
used_time = {}  # 创建一个计时字典


# debug输出函数
def user_debug(print_str, print_value='', ):
    """第一个参数为变量名称，第二个参数为变量的值"""
    if ucfg.debug == 1:
        if print_value:
            print(str(print_str) + ' = ' + str(print_value))
        else:
            print(str(print_str))


# 将通达信的日线文件转换成CSV格式保存函数。通达信数据文件32字节为一组。
def day2csv(source_dir, file_name, target_dir):
    """
    将通达信的日线文件转换成CSV格式保存函数。通达信数据文件32字节为一组
    :param source_dir: str 源文件路径
    :param file_name: str 文件名
    :param target_dir: str 要保存的路径
    :return: none
    """

    # 以二进制方式打开源文件
    source_path = source_dir + os.sep + file_name  # 源文件包含文件名的路径
    source_file = open(source_path, 'rb')
    buf = source_file.read()  # 读取源文件保存在变量中
    source_file.close()
    source_size = os.path.getsize(source_path)  # 获取源文件大小
    source_row_number = int(source_size / 32)
    # user_debug('源文件行数', source_row_number)

    # 打开目标文件，后缀名为CSV
    target_path = target_dir + os.sep + file_name[2:-4] + '.csv'  # 目标文件包含文件名的路径
    # user_debug('target_path', target_path)

    if not os.path.isfile(target_path):
        # 目标文件不存在。写入表头行。begin从0开始转换
        target_file = open(target_path, 'w', encoding="utf-8")  # 以覆盖写模式打开文件
        header = ',' + str('date') + ',' + str('open') + ',' + str('high') + ',' + str('low') + ',' \
                 + str('close') + ',' + str('vol') + ',' + str('amount')
        target_file.write(header)
        begin = 0
        end = begin + 32
        row_number = 0
    else:
        # 不为0，文件有内容。行附加。
        # 通达信数据32字节为一组，因此通达信文件大小除以32可算出通达信文件有多少行（也就是多少天）的数据。
        # 再用readlines计算出目标文件已有多少行（目标文件多了首行标题行），(行数-1)*32 即begin要开始的字节位置

        target_file = open(target_path, 'a+', encoding="utf-8")  # 以追加读写模式打开文件
        # target_size = os.path.getsize(target_path)  #获取目标文件大小

        # 由于追加读写模式载入文件后指针在文件的结尾，需要先把指针改到文件开头，读取文件行数。
        user_debug('当前指针', target_file.tell())
        target_file.seek(0, 0)  # 文件指针移到文件头
        user_debug('移动指针到开头', target_file.seek(0, 0))
        target_file_content = target_file.readlines()  # 逐行读取文件内容
        row_number = len(target_file_content)  # 获得文件行数
        user_debug('目标文件行数', row_number)
        user_debug('目标文件最后一行的数据', target_file_content[-1])
        target_file.seek(0, 2)  # 文件指针移到文件尾
        user_debug('移动指针到末尾', target_file.seek(0, 2))
        if row_number > source_row_number:
            user_debug('已是最新数据，跳过for循环')
        else:
            print('追加模式，从' + str(row_number + 1) + '行开始')

        if row_number == 0:  # 如果文件出错是0的特殊情况
            begin = 0
        else:
            row_number = row_number - 1  # 由于pandas的dataFrame格式索引从0开始，为下面for循环需要减1
            begin = row_number * 32

        end = begin + 32

    for i in range(row_number, source_row_number):
        # 由于pandas的dataFrame格式首行为标题行，第二行的索引从0开始，
        # 因此转换出来显示的行数比原本少一行，但实际数据一致
        #
        # 将字节流转换成Python数据格式
        # I: unsigned int
        # f: float
        # a[5]浮点类型的成交金额，使用decimal类四舍五入为整数
        a = unpack('IIIIIfII', buf[begin:end])
        line = '\n' + str(i) + ',' \
               + str(a[0]) + ',' \
               + str(a[1] / 100.0) + ',' \
               + str(a[2] / 100.0) + ',' \
               + str(a[3] / 100.0) + ',' \
               + str(a[4] / 100.0) + ',' \
               + str(a[6]) + ',' \
               + str(Decimal(a[5]).quantize(Decimal("1."), rounding="ROUND_HALF_UP"))
        target_file.write(line)
        begin += 32
        end += 32
    target_file.close()


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

# 处理沪市股票
file_list = os.listdir(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday')
used_time['sh_begintime'] = time.time()
for f in file_list:
    # 处理沪市sh6开头文件，否则跳过此次循环
    if f[0:3] == 'sh6':
        print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
        day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday', f, ucfg.tdx['csv_lday'])
used_time['sh_endtime'] = time.time()

# 处理深市股票
file_list = os.listdir(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday')
used_time['sz_begintime'] = time.time()
for f in file_list:
    if f[0:4] == 'sz00' or f[0:4] == 'sz30':  # 处理深市sh00开头和创业板sh30文件，否则跳过此次循环
        print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
        day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday', f, ucfg.tdx['csv_lday'])
used_time['sz_endtime'] = time.time()

# 处理指数文件
used_time['index_begintime'] = time.time()

for i in ucfg.index_list:
    print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + i)
    if 'sh' in i:
        day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday', i, ucfg.tdx['csv_index'])
    elif 'sz' in i:
        day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday', i, ucfg.tdx['csv_index'])

used_time['index_endtime'] = time.time()

print('沪市处理完毕，用时' + str(int(used_time['sh_endtime'] - used_time['sh_begintime'])) + '秒')
print('深市处理完毕，用时' + str(int(used_time['sz_endtime'] - used_time['sz_begintime'])) + '秒')
print('指数文件处理完毕，用时' + str(int(used_time['index_endtime'] - used_time['index_begintime'])) + '秒')

starttime_tick = time.time()
file_list = os.listdir(ucfg.tdx['csv_lday'])
df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})
for filename in file_list:
    process_info = f'[{(file_list.index(filename) + 1):>4}/{str(len(file_list))}] {filename}'
    df_bfq = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + filename, index_col=0, encoding='gbk')
    df_qfq = func_TDX.make_fq(filename[:-4], df_bfq, df_gbbq)
    df_qfq.to_csv(ucfg.tdx['csv_lday'] + os.sep + filename, index=False, encoding='gbk')

    print(f'{process_info} 完成 已用{(time.time() - starttime_tick):.2f}秒 剩余预计'
          f'{int((time.time() - starttime_tick) / (file_list.index(filename) + 1) * (len(file_list) - (file_list.index(filename) + 1)))}秒')