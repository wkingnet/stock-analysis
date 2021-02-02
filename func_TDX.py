#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
模仿通达信语句的函数库，如MA(C,5) REF(C,1)等样式。以及其他一些读取通达信相关的函数。
语句简单，只为了和通达信公式看起来一致，方便排查
输入类型最好是DataFrame Series类型
作者：wking [http://wkings.net]
"""
import os
import statistics
import time
import datetime
import pandas as pd
from retry import retry
import user_config as ucfg


def REF(value, day):
    """
    引用若干周期前的数据。可以是列表或序列类型
    """
    result = value[~day]
    return result


def MA(value, day):
    """
    返回简单移动平均。可以是列表或序列类型
    """
    result = statistics.mean(value[-day:])
    return result


def HHV(value, day):
    """
    返回最大值
    """
    value = max(value[-day:])
    return value


def LLV(value, day):
    """
    返回最小值
    """
    value = min(value[-day:])
    return value


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


def historyfinancialreader(filepath):
    """
    读取解析通达信目录的历史财务数据
    :param filepath: 字符串类型。传入文件路径
    :return: DataFrame格式。返回解析出的财务文件内容
    """
    import struct

    cw_file = open(filepath, 'rb')
    header_pack_format = '<1hI1H3L'
    header_size = struct.calcsize(header_pack_format)
    stock_item_size = struct.calcsize("<6s1c1L")
    data_header = cw_file.read(header_size)
    stock_header = struct.unpack(header_pack_format, data_header)
    max_count = stock_header[2]
    report_date = stock_header[1]
    report_size = stock_header[4]
    report_fields_count = int(report_size / 4)
    report_pack_format = '<{}f'.format(report_fields_count)
    results = []
    for stock_idx in range(0, max_count):
        cw_file.seek(header_size + stock_idx * struct.calcsize("<6s1c1L"))
        si = cw_file.read(stock_item_size)
        stock_item = struct.unpack("<6s1c1L", si)
        code = stock_item[0].decode("utf-8")
        foa = stock_item[2]
        cw_file.seek(foa)
        info_data = cw_file.read(struct.calcsize(report_pack_format))
        data_size = len(info_data)
        cw_info = list(struct.unpack(report_pack_format, info_data))
        cw_info.insert(0, code)
        results.append(cw_info)
    df = pd.DataFrame(results)
    return df


@retry(tries=3, delay=3)  # 无限重试装饰性函数
def dowload_url(url):
    """
    :param url:要下载的url
    :return: request.get实例化对象
    """
    import requests
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/87.0.4280.141',
    }
    response_obj = requests.get(url, headers=header, timeout=5)  # get方式请求
    response_obj.raise_for_status()  # 检测异常方法。如有异常则抛出，触发retry
    # print(f'{url} 下载完成')
    return response_obj


def list_local_cwfile(ext_name):
    """
    列出本地已有的专业财务文件。返回文件列表
    :param ext_name: str类型。文件扩展名。返回指定扩展名的文件列表
    :return: list类型。财务专业文件列表
    """
    cw_path = ucfg.tdx['tdx_path'] + os.sep + "vipdoc" + os.sep + "cw"
    tmplist = os.listdir(cw_path)  # 遍历通达信vipdoc/cw目录
    cw_filelist = []
    for file in tmplist:  # 只保留gpcw????????.扩展名 格式文件
        if len(file) == 16 and file[:4] == "gpcw" and file[-4:] == "." + ext_name:
            cw_filelist.append(file)
    # print(f'检测到{len(cw_filelist)}个专业财务文件')
    return cw_filelist


def make_fq(code, df_code, df_gbbq, start_date='', end_date='', fqtype='qfq'):
    """
    股票周期数据复权处理函数
    :param code:str格式，具体股票代码
    :param df_code:DF格式，未除权的具体股票日线数据。DF自动生成的数字索引，列定义：date,open,high,low,close,vol,amount
    :param df_gbbq:DF格式，通达信导出的全股票全日期股本变迁数据。DF读取gbbq文件必须加入dtype={'code': str}参数，否则股票代码开头0会忽略
    :param start_date:可选，要截取的起始日期。默认为空。格式"2020-10-10"
    :param end_date:可选，要截取的截止日期。默认为空。格式"2020-10-10"
    :param fqtype:可选，复权类型。默认前复权。
    :return:复权后的DF格式股票日线数据
    """
    import csv

    '''以下是从https://github.com/rainx/pytdx/issues/78#issuecomment-335668322 提取学习的前复权代码
    import datetime

    import numpy as np
    import pandas as pd
    from pytdx.hq import TdxHq_API
    # from pypinyin import lazy_pinyin
    import tushare as ts

    '除权除息'
    api = TdxHq_API()

    with api.connect('180.153.39.51', 7709):
        # 从服务器获取该股的股本变迁数据
        category = {
            '1': '除权除息', '2': '送配股上市', '3': '非流通股上市', '4': '未知股本变动', '5': '股本变化',
            '6': '增发新股', '7': '股份回购', '8': '增发新股上市', '9': '转配股上市', '10': '可转债上市',
            '11': '扩缩股', '12': '非流通股缩股', '13': '送认购权证', '14': '送认沽权证'}
        data = api.to_df(api.get_xdxr_info(0, '000001'))
        data = data \
            .assign(date=pd.to_datetime(data[['year', 'month', 'day']])) \
            .drop(['year', 'month', 'day'], axis=1) \
            .assign(category_meaning=data['category'].apply(lambda x: category[str(x)])) \
            .assign(code=str('000001')) \
            .rename(index=str, columns={'panhouliutong': 'liquidity_after',
                                        'panqianliutong': 'liquidity_before', 'houzongguben': 'shares_after',
                                        'qianzongguben': 'shares_before'}) \
            .set_index('date', drop=False, inplace=False)
        xdxr_data = data.assign(date=data['date'].apply(lambda x: str(x)[0:10]))  # 该股的股本变迁DF处理完成
        df_gbbq = xdxr_data[xdxr_data['category'] == 1]  # 提取只有除权除息的行保存到DF df_gbbq
        # print(df_gbbq)

        # 从服务器读取该股的全部历史不复权K线数据，保存到data表，  只包括 日期、开高低收、成交量、成交金额数据
        data = pd.concat([api.to_df(api.get_security_bars(9, 0, '000001', (9 - i) * 800, 800)) for i in range(10)], axis=0)

        # 从data表加工数据，保存到bfq_data表
        df_code = data \
            .assign(date=pd.to_datetime(data['datetime'].apply(lambda x: x[0:10]))) \
            .assign(code=str('000001')) \
            .set_index('date', drop=False, inplace=False) \
            .drop(['year', 'month', 'day', 'hour',
                   'minute', 'datetime'], axis=1)
        df_code['if_trade'] = True
        # 不复权K线数据处理完成，保存到bfq_data表

        # 提取info表的category列的值，按日期一一对应，列拼接到bfq_data表。也就是标识出当日是除权除息日的行
        data = pd.concat([df_code, df_gbbq[['category']][df_code.index[0]:]], axis=1)
        # print(data)

        data['date'] = data.index
        data['if_trade'].fillna(value=False, inplace=True)  # if_trade列，无效的值填充为False
        data = data.fillna(method='ffill')  # 向下填充无效值

        # 提取info表的'fenhong', 'peigu', 'peigujia',‘songzhuangu'列的值，按日期一一对应，列拼接到data表。
        # 也就是将当日是除权除息日的行，对应的除权除息数据，写入对应的data表的行。
        data = pd.concat([data, df_gbbq[['fenhong', 'peigu', 'peigujia',
                                      'songzhuangu']][df_code.index[0]:]], axis=1)
        data = data.fillna(0)  # 无效值填空0

        data['preclose'] = (data['close'].shift(1) * 10 - data['fenhong'] + data['peigu']
                            * data['peigujia']) / (10 + data['peigu'] + data['songzhuangu'])
        data['adj'] = (data['preclose'].shift(-1) / data['close']).fillna(1)[::-1].cumprod()  # 计算每日复权因子
        data['open'] = data['open'] * data['adj']
        data['high'] = data['high'] * data['adj']
        data['low'] = data['low'] * data['adj']
        data['close'] = data['close'] * data['adj']
        data['preclose'] = data['preclose'] * data['adj']

        data = data[data['if_trade']]
        result = data \
            .drop(['fenhong', 'peigu', 'peigujia', 'songzhuangu', 'if_trade', 'category'], axis=1)[data['open'] != 0] \
            .assign(date=data['date'].apply(lambda x: str(x)[0:10]))
        print(result)
    '''
    # 提取只有除权除息的行保存到DF df_gbbq
    df_gbbq = df_gbbq.loc[(df_gbbq['类别'] == '除权除息') & (df_gbbq['code'] == code)]
    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04 为了按日期一一对应拼接
    df_gbbq = df_gbbq.assign(date=pd.to_datetime(df_gbbq['权息日'], format='%Y%m%d'))  # 添加date列，设置为datetime64[ns]格式
    df_gbbq.set_index('date', drop=True, inplace=True)  # 设置权息日为索引  (字符串表示的日期 "19910101")
    df_gbbq['category'] = 1.0  # 添加category列

    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04  为了按日期一一对应拼接
    df_code['date'] = pd.to_datetime(df_code['date'], format='%Y%m%d')
    df_code.set_index('date', drop=True, inplace=True)
    df_code['if_trade'] = True

    # 提取info表的category列的值，按日期一一对应，列拼接到bfq_data表。也就是标识出当日是除权除息日的行
    data = pd.concat([df_code, df_gbbq[['category']][df_code.index[0]:]], axis=1)
    # print(data)

    data['if_trade'].fillna(value=False, inplace=True)  # if_trade列，无效的值填充为False
    data = data.fillna(method='ffill')  # 向下填充无效值

    # 提取info表的'fenhong', 'peigu', 'peigujia',‘songzhuangu'列的值，按日期一一对应，列拼接到data表。
    # 也就是将当日是除权除息日的行，对应的除权除息数据，写入对应的data表的行。
    data = pd.concat([data, df_gbbq[['分红-前流通盘', '配股-后总股本', '配股价-前总股本',
                                     '送转股-后流通盘']][df_code.index[0]:]], axis=1)
    data = data.fillna(0)  # 无效值填空0
    data['preclose'] = (data['close'].shift(1) * 10 - data['分红-前流通盘'] + data['配股-后总股本']
                        * data['配股价-前总股本']) / (10 + data['配股-后总股本'] + data['送转股-后流通盘'])
    data['adj'] = (data['preclose'].shift(-1) / data['close']).fillna(1)[::-1].cumprod()  # 计算每日复权因子
    data['open'] = data['open'] * data['adj']
    data['high'] = data['high'] * data['adj']
    data['low'] = data['low'] * data['adj']
    data['close'] = data['close'] * data['adj']
    # data['preclose'] = data['preclose'] * data['adj']  # 这行没用了
    data = data.round({'open': 2, 'high': 2, 'low': 2, 'close': 2, })  # 指定列四舍五入
    data = data[data['if_trade']]

    # 抛弃过程处理行
    data = data.drop(['分红-前流通盘', '配股-后总股本', '配股价-前总股本',
                      '送转股-后流通盘', 'if_trade', 'category', 'preclose', 'adj'], axis=1)[data['open'] != 0]
    # 复权处理完成

    # 计算换手率
    starttime_tick = time.time()
    cwfile_list = os.listdir(ucfg.tdx['csv_cw'])  # 遍历cw目录 生成文件名列表
    for cwfile in cwfile_list:  # 遍历所有文件
        cwfile_date = str(cwfile[4:-4])
        with open(ucfg.tdx['csv_cw'] + os.sep + cwfile) as fileobj:
            csvobj = csv.reader(fileobj)
            starttime_tick = time.time()
            for row in csvobj:  # 按行遍历CSV
                if row[1] == code:  # 如果第一列等于要找的股票
                    # 财务文件字段的对应列需+1
                    # print(f'{cwfile_date} 总股本:{row[239]} 流通股本:{row[267]}')
                    starttime_tick = time.time()
                    if row[267] == '0' or row[267] == '0.0':  # 如果csv文件的流通股值是0，退出CSV for循环 也就是打开下一个gpcw文件
                        break
                    while True:
                        try:  # 尝试日期对应行是否存在，不存在会抛出异常
                            data.loc[cwfile_date]
                        except:  # 日期对应行不存在
                            cwfile_date = datetime.datetime.strptime(cwfile_date, '%Y%m%d')  # 字符串转日期格式
                            cwfile_date = cwfile_date + datetime.timedelta(days=1)  # 加1天
                            cwfile_date = cwfile_date.strftime('%Y%m%d')  # 日期转字符串
                        else:
                            data.at[cwfile_date, '流通股'] = float(row[267])
                            break  # 退出while循环
                    print(f'while完成 用时{(time.time() - starttime_tick):.2f}秒')
                    break  # 退出CSV for循环
            print(f'遍历CSV完成 用时{(time.time() - starttime_tick):.2f}秒')
    print(f'导入财报完成 用时{(time.time() - starttime_tick):.2f}秒')
    data = data.fillna(method='ffill')  # 向下填充无效值
    data = data.fillna(method='bfill')  # 向上填充无效值  为了弥补开始几行的空值
    data['流通市值'] = data['流通股'] * data['close']
    data['换手率%'] = data['vol'] / data['流通股'] * 100

    if len(start_date) == 0 and len(end_date) == 0:
        pass
    elif len(start_date) != 0 and len(end_date) == 0:
        data = data[start_date:]
    elif len(start_date) == 0 and len(end_date) != 0:
        data = data[:end_date]
    elif len(start_date) != 0 and len(end_date) != 0:
        data = data[start_date:end_date]
    data = data.reset_index()  # 重置索引行，数字索引，date列到第一列，保存为str '1991-01-01' 格式
    return data


if __name__ == '__main__':
    df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})
    df_bfq = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + '000001.csv', index_col=0, encoding='gbk')
    df_qfq = make_fq('000001', df_bfq, df_gbbq)
