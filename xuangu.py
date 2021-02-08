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
import func_TDX
import user_config as ucfg

# 配置部分
start_date = ''
end_date = ''

# 变量定义
tdxpath = ucfg.tdx['tdx_path']
csvdaypath = ucfg.tdx['csv_lday']
已选出股票列表 = []  # 策略选出的股票
要剔除的通达信概念 = ["ST板块", ]  # list类型。通达信软件中查看“概念板块”。
要剔除的通达信行业 = ["T1002", ]  # list类型。记事本打开 通达信目录\incon.dat，查看#TDXNHY标签的行业代码。

starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()


# 主程序开始
# 要进行策略的股票列表筛选
print("生成股票列表")
stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_lday'])]  # 去文件名里的.csv，生成纯股票代码list
print("剔除通达信概念股票")
tmplist = []
df = func_TDX.get_TDX_blockfilecontent("block_gn.dat")
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
print("开始载入所有日线文件到内存")
dict = {}
for stockcode in stocklist:
    csvfile = csvdaypath + os.sep + stockcode + '.csv'
    dict[stockcode] = pd.read_csv(csvfile, encoding='gbk', index_col=None, dtype={'code': str})
    dict[stockcode]['date'] = pd.to_datetime(dict[stockcode]['date'], format='%Y-%m-%d')  # 转为时间格式
    dict[stockcode].set_index('date', drop=True, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    process_info = f'[{(stocklist.index(stockcode) + 1):>4}/{str(len(stocklist))}] {stockcode}'
    celue1 = CeLue.策略1(dict[stockcode], start_date=start_date, end_date=end_date)
    if celue1:
        celue2 = CeLue.策略2(dict[stockcode], start_date=start_date, end_date=end_date)
        if celue2:
            已选出股票列表.append(stockcode)
    print(f'{process_info} 完成，已选出 {len(已选出股票列表):>2d} 只股票 已用{(time.time() - starttime_tick):>5.2f}秒')

print(f'开始选股')
for stockcode in stocklist:
    # 留空
    pass
# 结果
print(f'全部完成，已选出{len(已选出股票列表)}只股票，清单:')
print(已选出股票列表)
