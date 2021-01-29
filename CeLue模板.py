"""
此为策略模板文件。你可自己写策略。个人实际策略不分享。
"""
from func_TDX import *


def 策略1(df, start_date='', end_date=''):
    """

    :param DataFrame df:输入具体一个股票的DataFrame数据表。时间列为索引。
    :param str start_date:可选。字符串类型。留空从头开始。"2020-10-10"格式，策略指定从某日期开始
    :param str end_date:可选。字符串类型。留空到末尾。"2020-10-10"格式，策略指定到某日期结束
    :return bool: 截止日期这天，策略是否触发。true触发，false不触发
    """
    if start_date == '':
        start_date = df.index[0]  # 设置为df第一个日期
    if end_date == '':
        end_date = df.index[-1]  # 设置为df最后一个日期
    换手率 = df['turn']  # 提取换手率为序列
    
    判断1 = df.at[end_date, 'close'] > 100  # 判断1，最新一天的收盘价大于100元

    if max(换手率[-200:]) < 15:  # 如果最近200天内的最大换手率小于15%
        判断2 = True
    else:
    	判断2 = False

    result = 判断1 and 判断2
    return result
