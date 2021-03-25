"""
为日线数据添加全部股票的历史策略买点列。
由于策略需要随时修改调整，因此单独写了策略写入文件，没有整合进readTDX_lday.py
"""
import os
import sys
import time
from multiprocessing import Pool, RLock, freeze_support
import numpy as np
import pandas as pd
from tqdm import tqdm
from rich import print

import CeLue  # 个人策略文件，不分享
import func_TDX
import user_config as ucfg

# 变量定义
要剔除的通达信概念 = ["ST板块", ]  # list类型。通达信软件中查看“概念板块”。
要剔除的通达信行业 = ["T1002", ]  # list类型。记事本打开 通达信目录\incon.dat，查看#TDXNHY标签的行业代码。


def lambda_update0(x):
    if type(x) == float:
        x = np.nan
    elif x == '0.0':
        x = np.nan
    return x


def celue_save(file_list, HS300_信号, tqdm_position=None):
    # print('\nRun task (%s)' % os.getpid())
    starttime_tick = time.time()
    df_celue = pd.DataFrame()
    if 'single' in sys.argv[1:]:
        tq = tqdm(file_list)
    else:
        tq = tqdm(file_list, leave=False, position=tqdm_position)
    for stockcode in tq:
        tq.set_description(stockcode)
        # process_info = f'[{(stocklist.index(stockcode) + 1):>4}/{str(len(stocklist))}] {stockcode}'
        pklfile = ucfg.tdx['pickle'] + os.sep + stockcode + ".pkl"
        df = pd.read_pickle(pklfile)
        if 'del' in sys.argv[1:]:
            if 'celue_buy' in df.columns:
                del df['celue_buy']
            if 'celue_sell' in df.columns:
                del df['celue_sell']
        df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
        if not {'celue_buy', 'celue_buy'}.issubset(df.columns):
            df.insert(df.shape[1], 'celue_buy', np.nan)  # 插入celue_buy列，赋值NaN
            df.insert(df.shape[1], 'celue_sell', np.nan)  # 插入celue_sell列，赋值NaN
        else:
            # 由于make_fq时fillna将最新的空的celue单元格也填充为0，所以先恢复nan
            df['celue_buy'] = (df['celue_buy']
                               .apply(lambda x: lambda_update0(x))
                               .mask(df['celue_buy'] == 'False', False)
                               .mask(df['celue_buy'] == 'True', True)
                               )

            df['celue_sell'] = (df['celue_sell']
                                .apply(lambda x: lambda_update0(x))
                                .mask(df['celue_sell'] == 'False', False)
                                .mask(df['celue_sell'] == 'True', True)
                                )

        if True in df['celue_buy'].isna().to_list():
            start_date = df.index[np.where(df['celue_buy'].isna())[0][0]]
            end_date = df.index[-1]
            celue2 = CeLue.策略2(df, HS300_信号, start_date=start_date, end_date=end_date)
            celue_sell = CeLue.卖策略(df, celue2, start_date=start_date, end_date=end_date)
            df.loc[start_date:end_date, 'celue_buy'] = celue2
            df.loc[start_date:end_date, 'celue_sell'] = celue_sell
            df.reset_index(drop=True, inplace=True)
            df.to_csv(ucfg.tdx['csv_lday'] + os.sep + stockcode + '.csv', index=False, encoding='gbk')
            df.to_pickle(ucfg.tdx['pickle'] + os.sep + stockcode + ".pkl")
        lefttime_tick = int((time.time() - starttime_tick) / (file_list.index(stockcode) + 1)
                            * (len(file_list) - (file_list.index(stockcode) + 1)))

        # 提取celue是true的列，单独保存到一个df，返回这个df
        df_celue = df_celue.append(df.loc[df['celue_buy'] | df['celue_sell']])
        # print(f'{process_info} 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')
    df_celue['date'] = pd.to_datetime(df_celue['date'], format='%Y-%m-%d')  # 转为时间格式
    df_celue.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并

    return df_celue


if __name__ == '__main__':
    print(f'附带命令行参数 del 完全重新生成策略信号, 参数 single 单进程执行(默认多进程)')
    df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
    df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
    df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    HS300_信号 = CeLue.策略HS300(df_hs300)
    stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['pickle'])]

    if 'del' in sys.argv[1:]:
        print(f'检测到参数 del, 完全重新生成策略信号')

    if 'single' in sys.argv[1:]:
        print(f'检测到参数 single, 单进程执行')
        df_celue = celue_save(stocklist, HS300_信号)
    else:
        # 多线程。好像没啥效果提升
        # threads = []
        # t_num = 4  # 线程数
        # for i in range(0, t_num):
        #     div = int(len(stocklist) / t_num)
        #     mod = len(stocklist) % t_num
        #     if i+1 != t_num:
        #         # print(i, i * div, (i + 1) * div)
        #         threads.append(threading.Thread(target=celue_save, args=(stocklist[i*div:(i+1)*div], HS300_信号)))
        #     else:
        #         # print(i, i * div, (i + 1) * div + mod)
        #         threads.append(threading.Thread(target=celue_save, args=(stocklist[i*div:(i+1)*div+mod], HS300_信号)))
        # # celue_save(stocklist, HS300_信号)
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
        # print('Parent process %s' % os.getpid())
        t_num = os.cpu_count() - 2  # 进程数 读取CPU逻辑处理器个数
        freeze_support()  # for Windows support
        tqdm.set_lock(RLock())  # for managing output contention
        p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
        pool_result = []  # 存放pool池的返回对象列表
        for i in range(0, t_num):
            div = int(len(stocklist) / t_num)
            mod = len(stocklist) % t_num
            if i + 1 != t_num:
                # print(i, i * div, (i + 1) * div)
                pool_result.append(p.apply_async(celue_save, args=(stocklist[i * div:(i + 1) * div], HS300_信号, i)))
            else:
                # print(i, i * div, (i + 1) * div + mod)
                pool_result.append(
                    p.apply_async(celue_save, args=(stocklist[i * div:(i + 1) * div + mod], HS300_信号, i)))
        # celue_save(stocklist, HS300_信号)

        # print('Waiting for all subprocesses done...')
        p.close()
        p.join()

        # 处理celue汇总.csv文件。保存为csv文件，方便查看
        df_celue = pd.DataFrame()
        # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
        for i in pool_result:
            df_celue = df_celue.append(i.get())

    # df_celue 是处理后的所有股票策略信号汇总文件。
    # 下面处理自定义股票板块剔除

    # 生成要剔除的股票列表 kicklist
    print(f'生成股票列表, 共 {len(stocklist)} 只股票')
    print(f'剔除通达信概念股票: {要剔除的通达信概念}')
    kicklist = []
    df = func_TDX.get_TDX_blockfilecontent("block_gn.dat")
    # 获取df中blockname列的值是ST板块的行，对应code列的值，转换为list。用filter函数与stocklist过滤，得出不包括ST股票的对象，最后转为list
    for i in 要剔除的通达信概念:
        kicklist = kicklist + df.loc[df['blockname'] == i]['code'].tolist()
    print(f'剔除通达信行业股票: {要剔除的通达信行业}')
    df = pd.read_csv(ucfg.tdx['tdx_path'] + os.sep + 'T0002' + os.sep + 'hq_cache' + os.sep + "tdxhy.cfg",
                     sep='|', header=None, dtype='object')
    for i in 要剔除的通达信行业:
        kicklist = kicklist + df.loc[df[2] == i][1].tolist()
    print("剔除科创板股票")
    tdx_stocks = pd.read_csv(ucfg.tdx['tdx_path'] + '/T0002/hq_cache/infoharbor_ex.code',
                             sep='|', header=None, index_col=None, encoding='gbk', dtype={0: str})
    kicklist = kicklist + tdx_stocks[0][tdx_stocks[0].apply(lambda x: x[0:2] == "68")].to_list()
    stocklist = list(filter(lambda i: i not in kicklist, stocklist))
    print(f'共 {len(stocklist)} 只候选股票')

    # df_celue 剔除在kicklist中的股票
    df_celue = df_celue[~df_celue['code'].isin(kicklist)]

    df_celue = (df_celue
                .drop(["open", "high", "low", "vol", "amount", "adj", "流通股", "流通市值", "换手率"], axis=1)
                .sort_index()
                .reset_index(drop=True)
                )
    df_celue.to_csv(ucfg.tdx['csv_gbbq'] + os.sep + 'celue汇总.csv', index=True, encoding='gbk')

    print(f'全部处理完成，程序退出')
