"""
选股文件。导入数据——执行策略——显示结果
为保证和通达信选股一致，需使用前复权数据
"""
import os
import csv
import time
import datetime
import pandas as pd

import CeLue  # 个人策略文件，不分享
import user_config as ucfg
import pytdx.cr
# 变量定义
tdxpath = ucfg.tdx['tdx_path']
csvdaypath = ucfg.baostock['csv_day_qfq']
已选出股票列表 = []  # 策略选出的股票
要剔除的通达信概念 = ["ST板块", ]  # list类型。通达信软件中查看“概念板块”。
要剔除的通达信行业 = ["T1002", ]  # list类型。记事本打开 通达信目录\incon.dat，查看#TDXNHY标签的行业代码。

starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()


def get_TDX_blockfilecontent(filename):
    """
    读取本机通达信板块文件，获取文件内容
    :rtype: object
    :param filename: 字符串类型。输入的文件名。
    :return: DataFrame类型
    """
    from pytdx.reader import block_reader, TdxFileNotFoundException
    if ucfg.tdx['tdx_path']:
        filepath = ucfg.tdx['tdx_path'] + os.sep + 'T0002' + os.sep + 'hq_cache' + os.sep + filename
        df = block_reader.BlockReader().get_df(filepath)
    else:
        print("user_config文件的tdx_path变量未配置，或未找到" + filename + "文件")
    return df


# 主程序开始
# 要进行策略的股票列表筛选
print("生成股票列表")
stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_day'])]  # 去文件名里的.csv，生成纯股票代码list
print("剔除通达信概念股票")
tmplist = []
df = get_TDX_blockfilecontent("block_gn.dat")
# 获取df中blockname列的值是ST板块的行，对应code列的值，转换为list。用filter函数与stocklist过滤，得出不包括ST股票的对象，最后转为list
for i in 要剔除的通达信概念:
    tmplist = tmplist + df.loc[df['blockname'] == i]['code'].tolist()
stocklist = list(filter(lambda i: i not in tmplist, stocklist))
print("剔除通达信行业股票")
tmplist = []
df = pd.read_csv(ucfg.tdx['tdx_path'] + os.sep + 'T0002' + os.sep + 'hq_cache' + os.sep + "tdxhy.cfg",
                 sep='|', header=None, dtype='object')
for i in 要剔除的通达信行业:
    tmplist = tmplist + df.loc[df[2] == i][1].tolist()
stocklist = list(filter(lambda i: i not in tmplist, stocklist))
print("剔除科创板股票")
tmplist = []
for stockcode in stocklist:
    if stockcode[:2] != '68':
        tmplist.append(stockcode)
stocklist = tmplist

# 策略部分
print("开始循环执行策略")
for stockcode in stocklist:
    process_info = f'[{(stocklist.index(stockcode) + 1):>4}/{str(len(stocklist))}] {stockcode}'
    csvfile = csvdaypath + os.sep + stockcode + '.csv'
    df = pd.read_csv(csvfile, encoding='gbk', index_col=0)
    df = df.set_index('date')
    cl1 = CeLue.策略1(df)
    if cl1:
        已选出股票列表.append(stockcode)
    print(f'{process_info} 完成，已选出 {len(已选出股票列表):>2d} 只股票 已用{(time.time() - starttime_tick):>5.2f}秒')

# 结果
print(f'全部完成，已选出{len(已选出股票列表)}只股票，清单:')
print(已选出股票列表)
