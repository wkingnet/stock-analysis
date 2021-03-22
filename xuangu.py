# TODO xuangu多线程版本(可执行，有BUG，2021-03-22日，000887的celue2应该出信号但并没有出，不知道什么原因，暂缓使用)
"""
选股文件。导入数据——执行策略——显示结果
为保证和通达信选股一致，需使用前复权数据
"""
import os
import sys
import time
import pandas as pd
from multiprocessing import Pool, RLock, freeze_support
from rich import print
from tqdm import tqdm
import CeLue  # 个人策略文件，不分享
import func_TDX
import user_config as ucfg

# 配置部分
start_date = ''
end_date = ''

# 变量定义
tdxpath = ucfg.tdx['tdx_path']
csvdaypath = ucfg.tdx['pickle']
已选出股票列表 = []  # 策略选出的股票
要剔除的通达信概念 = ["ST板块", ]  # list类型。通达信软件中查看“概念板块”。
要剔除的通达信行业 = ["T1002", ]  # list类型。记事本打开 通达信目录\incon.dat，查看#TDXNHY标签的行业代码。

starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime = time.time()
starttime_tick = time.time()


def make_stocklist():
    # 要进行策略的股票列表筛选
    stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_lday'])]  # 去文件名里的.csv，生成纯股票代码list
    print(f'生成股票列表, 共 {len(stocklist)} 只股票')
    print(f'剔除通达信概念股票: {要剔除的通达信概念}')
    tmplist = []
    df = func_TDX.get_TDX_blockfilecontent("block_gn.dat")
    # 获取df中blockname列的值是ST板块的行，对应code列的值，转换为list。用filter函数与stocklist过滤，得出不包括ST股票的对象，最后转为list
    for i in 要剔除的通达信概念:
        tmplist = tmplist + df.loc[df['blockname'] == i]['code'].tolist()
    stocklist = list(filter(lambda i: i not in tmplist, stocklist))
    print(f'剔除通达信行业股票: {要剔除的通达信行业}')
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
    return stocklist


def load_dict_stock(stocklist):
    dicttemp = {}
    starttime_tick = time.time()
    tq = tqdm(stocklist)
    for stockcode in tq:
        tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        # dict[stockcode] = pd.read_csv(csvfile, encoding='gbk', index_col=None, dtype={'code': str})
        dicttemp[stockcode] = pd.read_pickle(pklfile)
    print(f'载入完成 用时 {(time.time() - starttime_tick):.2f} 秒')
    return dicttemp


def run_celue1(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        tq = tqdm(stocklist[:], leave=False, position=tqdm_position)
    for stockcode in tq:
        tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        df_stock = pd.read_pickle(pklfile)
        if df_today is not None:  # 更新当前最新行情，否则用昨天的数据
            df_stock = func_TDX.update_stockquote(stockcode, df_stock, df_today)
        df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
        df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
        celue1 = CeLue.策略1(df_stock, start_date=start_date, end_date=end_date, mode='fast')
        if not celue1:
            stocklist.remove(stockcode)
    return stocklist


def run_celue2(stocklist, HS300_信号, df_gbbq, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        tq = tqdm(stocklist[:], leave=False, position=tqdm_position)
    for stockcode in tq:
        tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        df_stock = pd.read_pickle(pklfile)
        if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '16:00:00':
            df_today_code = df_today.loc[df_today['code'] == stockcode]
            df_stock = func_TDX.update_stockquote(stockcode, df_stock, df_today_code)
            # 判断今天是否在该股的权息日内。如果是，需要重新前复权
            now_date = pd.to_datetime(time.strftime("%Y-%m-%d", time.localtime()))
            if now_date in df_gbbq.loc[df_gbbq['code'] == stockcode]['权息日'].to_list():
                cw_dict = func_TDX.readall_local_cwfile()
                df_stock = func_TDX.make_fq(stockcode, df_stock, df_gbbq, cw_dict)
        celue2 = CeLue.策略2(df_stock, HS300_信号, start_date=start_date, end_date=end_date).iat[-1]
        if not celue2:
            stocklist.remove(stockcode)
    return stocklist


# 主程序开始
if __name__ == '__main__':
    print(f'附带命令行参数 single 单进程执行(默认多进程)')
    stocklist = make_stocklist()
    print(f'共 {len(stocklist)} 只候选股票')

    # print("开始载入日线文件到内存")
    # df_dict = load_dict_stock(stocklist)

    df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})

    # 策略部分
    # 先判断今天是否买入
    print('今日HS300行情判断')
    df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
    df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
    df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '16:00:00':
        df_today = func_TDX.get_tdx_lastestquote((1, '000300'))
        df_hs300 = func_TDX.update_stockquote('000300', df_hs300, df_today)
        del df_today
    HS300_信号 = CeLue.策略HS300(df_hs300)
    if not HS300_信号.iat[-1]:
        print('今日HS300不满足买入条件，仍然选股，但不执行买入操作')
    else:
        print('今日HS300满足买入条件，执行买入操作')

    # 周一到周五，14点半到16点之间，获取在线行情。其他时间不是交易日，默认为离线数据已更新到最新
    df_today_tmppath = ucfg.tdx['csv_gbbq'] + '/df_today.pkl'
    if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '16:00:00' \
            and 0 <= time.localtime(time.time()).tm_wday <= 4:
        # 获取当前最新行情，临时保存到本地，防止多次调用被服务器封IP。
        if os.path.exists(df_today_tmppath):
            if round(time.time() - os.path.getmtime(df_today_tmppath)) < 600:  # 据创建时间小于10分钟读取本地文件
                print(f'读取本地临时最新行情文件')
                df_today = pd.read_pickle(df_today_tmppath)
            else:
                df_today = func_TDX.get_tdx_lastestquote(stocklist)
                df_today.to_pickle(df_today_tmppath, compression=None)
        else:
            df_today = func_TDX.get_tdx_lastestquote(stocklist)
            df_today.to_pickle(df_today_tmppath, compression=None)
    else:
        try:
            os.remove(df_today_tmppath)
        except FileNotFoundError:
            pass
        df_today = None

    print(f'开始执行策略1(mode=fast)')
    starttime_tick = time.time()
    if 'single' in sys.argv[1:]:
        print(f'检测到参数 single, 单进程执行')
        stocklist = run_celue1(stocklist, df_today)
    else:
        # 由于df_dict字典占用超多内存资源，导致多进程效率还不如单进程
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
                pool_result.append(p.apply_async(run_celue1, args=(stocklist[i * div:(i + 1) * div], df_today, i,)))
            else:
                # print(i, i * div, (i + 1) * div + mod)
                pool_result.append(p.apply_async(run_celue1, args=(stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

        # print('Waiting for all subprocesses done...')
        p.close()
        p.join()

        stocklist = []
        # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
        for i in pool_result:
            stocklist = stocklist + i.get()

    print(f'策略1执行完毕，已选出 {len(stocklist):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
    # print(stocklist)

    print(f'开始执行策略2')
    # 如果没有df_today
    if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '16:00:00' and 'df_today' not in dir():
        df_today = func_TDX.get_tdx_lastestquote(stocklist)  # 获取当前最新行情

    starttime_tick = time.time()
    if 'single' in sys.argv[1:]:
        print(f'检测到参数 single, 单进程执行')
        stocklist = run_celue2(stocklist, HS300_信号, df_gbbq, df_today)
    else:
        # 由于df_dict字典占用超多内存资源，导致多进程效率还不如单进程
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
                pool_result.append(p.apply_async(run_celue2, args=(stocklist[i * div:(i + 1) * div], HS300_信号, df_gbbq, df_today, i,)))
            else:
                # print(i, i * div, (i + 1) * div + mod)
                pool_result.append(p.apply_async(run_celue2, args=(stocklist[i * div:(i + 1) * div + mod], HS300_信号, df_gbbq, df_today, i,)))

        # print('Waiting for all subprocesses done...')
        p.close()
        p.join()

        stocklist = []
        # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
        for i in pool_result:
            stocklist = stocklist + i.get()

    print(f'策略2执行完毕，已选出 {len(stocklist):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')

    # 结果
    print(f'全部完成 共用时{(time.time() - starttime):>.2f}秒 已选出 {len(stocklist)} 只股票:')
    print(stocklist)
