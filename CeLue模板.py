"""
此为策略模板文件。你自己写策略后，一定要保存为celue.py
celue.py文件不直接执行，通过xuangu.py或celue_save.py调用

个人实际策略不分享。

MA函数返回的是值。
其余函数输入、输出都是序列。只有序列才能表现出来和通达信一样的判断逻辑。
HHV/LLV/COUNT使用了rolling函数，性能极差，慎用。

"""
import numpy as np
import talib
import time
import func
from func_TDX import rolling_window, REF, MA, SMA, HHV, LLV, COUNT, EXIST, CROSS, BARSLAST
from rich import print


def 策略HS300(df_hs300, start_date='', end_date=''):
    """
    HS300信号的作用是，当信号是0时，当日不买股票，1时买入。传出
    :param start_date:
    :param end_date:
    :return: 布尔序列
    """
    if start_date == '':
        start_date = df_hs300.index[0]  # 设置为df第一个日期
    if end_date == '':
        end_date = df_hs300.index[-1]  # 设置为df最后一个日期
    df_hs300 = df_hs300.loc[start_date:end_date]
    HS300_CLOSE = df_hs300['close']
    HS300_当日涨幅 = (HS300_CLOSE / REF(HS300_CLOSE, 1) - 1) * 100
    HS300_信号 = ~(HS300_当日涨幅 < -1.5) & ~(HS300_当日涨幅 > 1.5)
    return HS300_信号


def 策略1(df, start_date='', end_date='', mode=None):
    """

    :param DataFrame df:输入具体一个股票的DataFrame数据表。时间列为索引。
    :param mode :str 'fast'为快速模式，只处理当日数据，用于开盘快速筛选股票。和策略2结合使用时不能用fast模式
    :param date start_date:可选。留空从头开始。2020-10-10格式，策略指定从某日期开始
    :param date end_date:可选。留空到末尾。2020-10-10格式，策略指定到某日期结束
    :return : 布尔序列
    """
    if start_date == '':
        start_date = df.index[0]  # 设置为df第一个日期
    if end_date == '':
        end_date = df.index[-1]  # 设置为df最后一个日期
    df = df.loc[start_date:end_date]

    O = df['open']
    H = df['high']
    L = df['low']
    C = df['close']
    if {'换手率'}.issubset(df.columns):  # 无换手率列的股票，只可能是近几个月的新股。
        换手率 = df['换手率']
    else:
        换手率 = 0

    if mode == 'fast':
        # 天数不足500天，收盘价小于9直接返回FALSE

        # TJ01
        # now_date = pd.to_datetime(time.strftime("%Y-%m-%d", time.localtime()))
        # if df.at[df.index[-1], 'date'] < now_date:
        #     print(f"{df.at[df.index[-1], 'code']} 无今日数据 跳过")
        #     return False
        # del now_date
        if C.shape[0] < 500 or C.iat[-1] < 9:
            return False

        金额万均 = MA(df['amount'] / 10000, 30)
        流通市值亿 = df['流通市值'] / 100000000

        MA5 = MA(C, 5)

        # TJ04
        # {排除当日涨停的股票}
        if df['code'][0][0:2] == "68" or df['code'][0][0:2] == "30":
            TJ04_1 = 1.2
        else:
            TJ04_1 = 1.1
        TJ04_2 = ~((C+0.01) >= np.ceil((np.floor(REF(C, 1)*1000*TJ04_1)-4)/10)/100)
        TJ04 = TJ04_2.iat[-1]

        result = TJ04
    else:
        金额万均 = SMA(df['amount'] / 10000, 30)
        流通市值亿 = df['流通市值'] / 100000000
        MA5 = SMA(C, 5)

        # TJ01
        TJ01 = (BARSLAST(C == 0) > 500) & (df['close'] > 9)

        # TJ04
        if df['code'][0][0:2] == "68" or df['code'][0][0:2] == "30":
            TJ04_1 = 1.2
        else:
            TJ04_1 = 1.1
        TJ04_2 = ~((C+0.01) >= np.ceil((np.floor(REF(C, 1)*1000*TJ04_1)-4)/10)/100)
        TJ04 = TJ04_2

        result = TJ01 & TJ04
    return result


def 策略2(df, HS300_信号, start_date='', end_date=''):
    """

    :param DataFrame df:输入具体一个股票的DataFrame数据表。时间列为索引。
    :param date start_date:可选。留空从头开始。2020-10-10格式，策略指定从某日期开始
    :param date end_date:可选。留空到末尾。2020-10-10格式，策略指定到某日期结束
    :return bool: 截止日期这天，策略是否触发。true触发，false不触发
    """

    if start_date == '':
        start_date = df.index[0]  # 设置为df第一个日期
    if end_date == '':
        end_date = df.index[-1]  # 设置为df最后一个日期
    df = df.loc[start_date:end_date]

    if df.shape[0] < 251:  # 小于250日 直接返回flase序列
        return pd.Series(index=df.index, dtype=bool)

    # 根据df的索引重建HS300信号，为了与股票交易日期一致
    HS300_信号 = pd.Series(HS300_信号, index=df.index, dtype=bool).dropna()

    O = df['open']
    H = df['high']
    L = df['low']
    C = df['close']
    换手率 = df['换手率']

    # 变量定义
    MA5 = SMA(C, 5)
    MA10 = SMA(C, 10)
    MA20 = SMA(C, 20)
    MA60 = SMA(C, 60)
    MA120 = SMA(C, 120)
    MA250 = SMA(C, 250)

    流通市值亿 = df['流通市值'] / 100000000

    # 判断部分
    # 每个TJ0?返回的都是bool序列

    # TJ01
    TJ01 = (MA120 > -5) & (MA10 < 60) & (MA60 < 10) & (-7 < MA250) & (MA250 < 10)

    # TJ02
    TJ02 = (C > SMA(C, 60)) & (C < SMA(C, 60) * 1.1) & (C > O)

    # TJ06
    # {20日/200日涨幅小于50%，且收盘价到上穿MA60日的涨幅 除以上穿MA60日到30日收盘最低价 的比 小于1.5倍 }
    TJ06_1 = LLV(C, 200)
    TJ06_2 = LLV(C, 20)
    TJ06_MA60_DAY = BARSLAST((REF(C, 5) < MA60) & CROSS(C, MA60))
    # TJ06_MA60 = REF(MA60, TJ06_MA60_DAY)
    # TJ06_MA60特殊，只能单独写出来
    TJ06_MA60 = pd.Series(index=TJ06_MA60_DAY.index, dtype=float)  # 新建序列，传递索引

    i = 0
    for k, v in TJ06_MA60_DAY.iteritems():
        if i - v > 0:
            TJ06_MA60.iat[i] = MA60.iat[i - v]
        i = i + 1

    df = pd.concat([df, TJ06_MA60_DAY.rename('TJ06_MA60_DAY')], axis=1)
    df.insert(df.shape[1], 'TJ06_MA60_LLV', np.NaN)
    for index_date in df.loc[df['TJ06_MA60_DAY'] == 0].index.to_list():
        index_int = df.index.get_loc(index_date)
        df.at[index_date, 'TJ06_MA60_LLV'] = df.iloc[index_int - 20:index_int]['close'].min()
    df = df.fillna(method='ffill')  # 向下填充无效值
    TJ06_MA60_LLV = df['TJ06_MA60_LLV']

    TJ06_3 = TJ06_MA60 / TJ06_MA60_LLV
    TJ06_4 = C / TJ06_MA60
    TJ06 = (TJ06_2 / TJ06_1 - 1 < 0.5) & (1 < TJ06_3 / TJ06_4) & (TJ06_3 / TJ06_4 < 1.5)

    # TJ11
    TJP1 = 策略1(df, start_date, end_date)
    TJ11_1 = HS300_信号 & TJP1 & TJ01 & TJ02 & TJ06
    TJ11_2 = COUNT(TJ11_1, 10)
    TJ11 = TJ11_1 & (REF(TJ11_2, 1) == 0)

    # TJ99
    TJ99 = TJ11

    # {输出部分}
    BUYSIGN = TJ99

    return BUYSIGN


def 卖策略(df, 策略2, start_date='', end_date=''):
    """

    :param df: 个股Dataframe
    :param 策略2: 买入策略2
    :param start_date:
    :param end_date:
    :return: 卖出策略序列
    """

    if True not in 策略2.to_list():  # 买入策略2 没有买入点
        return pd.Series(index=策略2.index, dtype=bool)

    if start_date == '':
        start_date = df.index[0]  # 设置为df第一个日期
    if end_date == '':
        end_date = df.index[-1]  # 设置为df最后一个日期
    df = df.loc[start_date:end_date]

    O = df['open']
    H = df['high']
    L = df['low']
    C = df['close']
    流通市值亿 = df['流通市值'] / 100000000

    # 变量定义
    MA10 = SMA(C, 10)
    MA60 = SMA(C, 60)

    BUY_TODAY = BARSLAST(策略2)
    BUY_PRICE_CLOSE = pd.Series(index=C.index, dtype=float)
    BUY_PRICE_OPEN = pd.Series(index=C.index, dtype=float)
    BUY_PCT = pd.Series(index=C.index, dtype=float)
    BUY_PCT_MAX = pd.Series(index=C.index, dtype=float)
    # C序列选择BUY_TODAY==0（当日为买入日）的索引的值列表，再倒序循环
    for i in BUY_TODAY[BUY_TODAY == 0].index.to_list()[::-1]:
        BUY_PRICE_CLOSE.loc[i] = C.loc[i]
        BUY_PRICE_OPEN.loc[i] = O.loc[i]
        BUY_PRICE_CLOSE.fillna(method='ffill', inplace=True)  # 向下填充无效值
        BUY_PRICE_OPEN.fillna(method='ffill', inplace=True)  # 向下填充无效值
        BUY_PCT = C / BUY_PRICE_CLOSE - 1
        # 循环计算BUY_PCT_MAX
        for k, v in BUY_PCT[i:].items():
            if np.isnan(BUY_PCT_MAX[k]):
                BUY_PCT_MAX[k] = BUY_PCT[i:k].max()

    # SELL01
    # {买入后，跌破MA60且跌破买入日的开盘价}
    SELL01 = (C < MA60) & (C < BUY_PRICE_OPEN)

    # SELL02
    # {最高点小于前低点，表示有向下跳空缺口}
    SELL02 = (BUY_PCT < 0.1) & (H < REF(L, 1))

    # SELL03
    # {买入N天后，涨幅大于0%，小于3%（N=流通市值亿)，当日收盘卖出}
    SELL03_1 = pd.Series(index=C.index, dtype=float)
    SELL03_1 = 流通市值亿.apply(lambda x: 7 if x < 100 else 14)
    SELL03 = (BUY_TODAY > SELL03_1) & (0.01 < C / BUY_PCT) & (C / BUY_PCT < 0.03)

    # SELLSIGN
    SELLSIGN01 = SELL01 | SELL02 | SELL03
    SELLSIGN = pd.Series(index=C.index, dtype=bool)
    # 循环，第一次出现SELLSIGN01=True时，SELLSIGN[k] = True并结束循环。可以获得和通达信公式AUTOFILTER相同效果
    for i in BUY_TODAY[BUY_TODAY == 0].index.to_list()[::-1]:
        for k, v in SELLSIGN01[i:].items():
            # k != i 排除买入信号当日同时产生卖出信号的极端情况
            if k != i and SELLSIGN01[k]:
                SELLSIGN[k] = True
                break

    return SELLSIGN


if __name__ == '__main__':
    # 调试用代码. 此文件不直接执行。通过xuangu.py或celue_save.py调用
    import pandas as pd
    import os
    import user_config as ucfg

    stock_code = '000887'
    start_date = ''
    end_date = ''
    df_stock = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + stock_code + '.csv',
                           index_col=None, encoding='gbk', dtype={'code': str})
    df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
    df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并

    df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
    df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
    df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '16:00:00':
        df_today = func.get_tdx_lastestquote((1, '000300'))
        df_hs300 = func.update_stockquote('000300', df_hs300, df_today)
    HS300_信号 = 策略HS300(df_hs300)

    if not HS300_信号.iat[-1]:
        print('今日HS300不满足买入条件，停止选股')

    if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '16:00:00':
        df_today = func.get_tdx_lastestquote(stock_code)
        df_stock = func.update_stockquote(stock_code, df_stock, df_today)
    celue1_fast = 策略1(df_stock, mode='fast', start_date=start_date, end_date=end_date)
    celue1 = 策略1(df_stock, mode='', start_date=start_date, end_date=end_date)
    celue2 = 策略2(df_stock, HS300_信号, start_date=start_date, end_date=end_date)
    celue_sell = 卖策略(df_stock, celue2, start_date=start_date, end_date=end_date)
    print(f'{stock_code} celue1_fast={celue1_fast} celue1={celue1.iat[-1]} celue2={celue2.iat[-1]} celue_sell={celue_sell.iat[-1]}')
