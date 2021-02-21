"""
为日线数据添加全部股票的历史策略买点列。
"""
import os
import csv
import time
import datetime
import numpy as np
import pandas as pd

import CeLue  # 个人策略文件，不分享
import func_TDX
import user_config as ucfg

df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
HS300_信号 = CeLue.策略HS300(df_hs300)
file_list = os.listdir(ucfg.tdx['pickle'])
starttime_tick = time.time()
for filename in file_list:
    process_info = f'[{(file_list.index(filename) + 1):>4}/{str(len(file_list))}] {filename}'
    pklfile = ucfg.tdx['pickle'] + os.sep + filename
    df = pd.read_pickle(pklfile)
    df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    if not {'celue'}.issubset(df.columns):
        df.insert(df.shape[1], 'celue', np.nan)  # 插入celu2列，赋值NaN
    if True in df['celue'].isna().to_list():
        start_date = df.index[np.where(df['celue2'].isna())[0][0]]
        end_date = df.index[-1]
        celue2 = CeLue.策略2(df, HS300_信号, start_date=start_date, end_date=end_date)
        df.loc[start_date:end_date, 'celue2'] = celue2
        df.reset_index(drop=True, inplace=True)
        df.to_csv(ucfg.tdx['csv_lday'] + os.sep + filename[:-4] + '.csv', index=False, encoding='gbk')
        df.to_pickle(ucfg.tdx['pickle'] + os.sep + filename)
    lefttime_tick = int((time.time() - starttime_tick) / (file_list.index(filename) + 1)
                        * (len(file_list) - (file_list.index(filename) + 1)))
    print(f'{process_info} 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')
