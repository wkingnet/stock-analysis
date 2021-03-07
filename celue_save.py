"""
为日线数据添加全部股票的历史策略买点列。
由于策略需要随时修改调整，因此单独写了策略写入文件，没有整合进readTDX_lday.py
"""
import os
import sys
import time
import threading
from multiprocessing import Pool, RLock, freeze_support
import numpy as np
import pandas as pd
from tqdm import tqdm

import CeLue  # 个人策略文件，不分享
import func_TDX
import user_config as ucfg


def celue_save(file_list, HS300_信号, tqdm_position=None):
    # print('\nRun task (%s)' % os.getpid())
    starttime_tick = time.time()
    df_celue = pd.DataFrame()
    tq = tqdm(file_list, position=tqdm_position)
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

        # 提取celue是true的列，单独保存到一个df，返回这个df
        df_celue = df_celue.append(df.loc[df['celue_buy'] | df['celue_sell']])
        # print(f'{process_info} 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')
    return df_celue


if __name__ == '__main__':
    df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
    df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
    df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    HS300_信号 = CeLue.策略HS300(df_hs300)
    file_list = os.listdir(ucfg.tdx['pickle'])

    # celue_save(file_list, HS300_信号)

    # 多线程。好像没啥效果提升
    # threads = []
    # t_num = 4  # 线程数
    # for i in range(0, t_num):
    #     div = int(len(file_list) / t_num)
    #     mod = len(file_list) % t_num
    #     if i+1 != t_num:
    #         # print(i, i * div, (i + 1) * div)
    #         threads.append(threading.Thread(target=celue_save, args=(file_list[i*div:(i+1)*div], HS300_信号)))
    #     else:
    #         # print(i, i * div, (i + 1) * div + mod)
    #         threads.append(threading.Thread(target=celue_save, args=(file_list[i*div:(i+1)*div+mod], HS300_信号)))
    # # celue_save(file_list, HS300_信号)
    # 
    # print(threads)
    # for t in threads:
    #     t.setDaemon(True)
    #     t.start()
    # 
    # for t in threads:
    #     t.join()
    # print("\n")

    # 多进程
    print('Parent process %s' % os.getpid())
    t_num = os.cpu_count() - 2  # 进程数 读取CPU逻辑处理器个数
    freeze_support()  # for Windows support
    tqdm.set_lock(RLock())  # for managing output contention
    p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
    pool_result = []  # 存放pool池的返回对象列表
    for i in range(0, t_num):
        div = int(len(file_list) / t_num)
        mod = len(file_list) % t_num
        if i + 1 != t_num:
            # print(i, i * div, (i + 1) * div)
            pool_result.append(p.apply_async(celue_save, args=(file_list[i * div:(i + 1) * div], HS300_信号, i)))
        else:
            # print(i, i * div, (i + 1) * div + mod)
            pool_result.append(p.apply_async(celue_save, args=(file_list[i * div:(i + 1) * div + mod], HS300_信号, i)))
    # celue_save(file_list, HS300_信号)

    # print('Waiting for all subprocesses done...')
    p.close()
    p.join()

    # 处理celue汇总.csv文件。保存为csv文件，方便查看
    df_celue = pd.DataFrame()
    # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
    for i in pool_result:
        df_celue = df_celue.append(i.get())
    df_celue = df_celue.sort_index().reset_index(drop=True)
    df_celue.to_csv(ucfg.tdx['csv_gbbq'] + os.sep + 'celue汇总.csv', index=True, encoding='gbk')
