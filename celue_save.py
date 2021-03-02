"""
为日线数据添加全部股票的历史策略买点列。
由于策略需要随时修改调整，因此单独写了策略写入文件，没有整合进readTDX_lday.py
"""
import os
import sys
import time
import threading
import numpy as np
import pandas as pd
from tqdm import tqdm
import CeLue  # 个人策略文件，不分享
import func_TDX
import user_config as ucfg


def celue_save(file_list, HS300_信号):
    starttime_tick = time.time()
    tq = tqdm(file_list)
    for filename in tq:
        tq.set_description(filename[:-4])
        # process_info = f'[{(file_list.index(filename) + 1):>4}/{str(len(file_list))}] {filename}'
        pklfile = ucfg.tdx['pickle'] + os.sep + filename
        df = pd.read_pickle(pklfile)
        if 'del' in str(sys.argv[1:]):
            del df['celue_buy']
            del df['celue_sell']
        df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
        if not {'celue_buy', 'celue_buy'}.issubset(df.columns):
            df.insert(df.shape[1], 'celue_buy', np.nan)  # 插入celu2列，赋值NaN
            df.insert(df.shape[1], 'celue_sell', np.nan)  # 插入celu2列，赋值NaN
        else:
            # 由于make_fq时fillna将最新的空的celue单元格也填充为0，所以先用循环恢复nan
            for i in range(1, df.shape[0]):
                if type(df.at[df.index[-i], 'celue_buy']) == float:
                    df.at[df.index[-i], 'celue_buy'] = np.nan
                    df.at[df.index[-i], 'celue_sell'] = np.nan
                else:
                    break
        if True in df['celue_buy'].isna().to_list():
            start_date = df.index[np.where(df['celue_buy'].isna())[0][0]]
            end_date = df.index[-1]
            celue2 = CeLue.策略2(df, HS300_信号, start_date=start_date, end_date=end_date)
            celue_sell = CeLue.卖策略(df, celue2, start_date=start_date, end_date=end_date)
            df.loc[start_date:end_date, 'celue_buy'] = celue2
            df.loc[start_date:end_date, 'celue_sell'] = celue_sell
            df.reset_index(drop=True, inplace=True)
            df.to_csv(ucfg.tdx['csv_lday'] + os.sep + filename[:-4] + '.csv', index=False, encoding='gbk')
            df.to_pickle(ucfg.tdx['pickle'] + os.sep + filename)
        lefttime_tick = int((time.time() - starttime_tick) / (file_list.index(filename) + 1)
                            * (len(file_list) - (file_list.index(filename) + 1)))
        # print(f'{process_info} 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')


if __name__ == '__main__':
    df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
    df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
    df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    HS300_信号 = CeLue.策略HS300(df_hs300)
    file_list = os.listdir(ucfg.tdx['pickle'])
    celue_save(file_list, HS300_信号)