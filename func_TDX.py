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
import numpy as np
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


# debug输出函数
def user_debug(print_str, print_value='', ):
    """第一个参数为变量名称，第二个参数为变量的值"""
    if ucfg.debug:
        if print_value:
            print(str(print_str) + ' = ' + str(print_value))
        else:
            print(str(print_str))


# 将通达信的日线文件转换成CSV格式保存函数。通达信数据文件32字节为一组。
def day2csv(source_dir, file_name, target_dir):
    """
    将通达信的日线文件转换成CSV格式保存函数。通达信数据文件32字节为一组
    :param source_dir: str 源文件路径
    :param file_name: str 文件名
    :param target_dir: str 要保存的路径
    :return: none
    """
    from struct import unpack
    from decimal import Decimal  # 用于浮点数四舍五入

    # 以二进制方式打开源文件
    source_path = source_dir + os.sep + file_name  # 源文件包含文件名的路径
    source_file = open(source_path, 'rb')
    buf = source_file.read()  # 读取源文件保存在变量中
    source_file.close()
    source_size = os.path.getsize(source_path)  # 获取源文件大小
    source_row_number = int(source_size / 32)
    # user_debug('源文件行数', source_row_number)

    # 打开目标文件，后缀名为CSV
    target_path = target_dir + os.sep + file_name[2:-4] + '.csv'  # 目标文件包含文件名的路径
    # user_debug('target_path', target_path)

    if not os.path.isfile(target_path):
        # 目标文件不存在。写入表头行。begin从0开始转换
        target_file = open(target_path, 'w', encoding="utf-8")  # 以覆盖写模式打开文件
        header = str('date') + ',' + str('open') + ',' + str('high') + ',' + str('low') + ',' \
                 + str('close') + ',' + str('vol') + ',' + str('amount')
        target_file.write(header)
        begin = 0
        end = begin + 32
        row_number = 0
    else:
        # 不为0，文件有内容。行附加。
        # 通达信数据32字节为一组，因此通达信文件大小除以32可算出通达信文件有多少行（也就是多少天）的数据。
        # 再用readlines计算出目标文件已有多少行（目标文件多了首行标题行），(行数-1)*32 即begin要开始的字节位置

        target_file = open(target_path, 'a+', encoding="gbk")  # 以追加读写模式打开文件
        # target_size = os.path.getsize(target_path)  #获取目标文件大小

        # 由于追加读写模式载入文件后指针在文件的结尾，需要先把指针改到文件开头，读取文件行数。
        user_debug('当前指针', target_file.tell())
        target_file.seek(0, 0)  # 文件指针移到文件头
        user_debug('移动指针到开头', target_file.seek(0, 0))
        target_file_content = target_file.readlines()  # 逐行读取文件内容
        row_number = len(target_file_content)  # 获得文件行数
        user_debug('目标文件行数', row_number)
        user_debug('目标文件最后一行的数据', target_file_content[-1])
        target_file.seek(0, 2)  # 文件指针移到文件尾
        user_debug('移动指针到末尾', target_file.seek(0, 2))
        if row_number > source_row_number:
            user_debug('已是最新数据，跳过for循环')
        else:
            print('追加模式，从' + str(row_number + 1) + '行开始')

        if row_number == 0:  # 如果文件出错是0的特殊情况
            begin = 0
        else:
            row_number = row_number - 1  # 由于pandas的dataFrame格式索引从0开始，为下面for循环需要减1
            begin = row_number * 32

        end = begin + 32

    for i in range(row_number, source_row_number):
        # 由于pandas的dataFrame格式首行为标题行，第二行的索引从0开始，
        # 因此转换出来显示的行数比原本少一行，但实际数据一致
        #
        # 将字节流转换成Python数据格式
        # I: unsigned int
        # f: float
        # a[5]浮点类型的成交金额，使用decimal类四舍五入为整数
        a = unpack('IIIIIfII', buf[begin:end])
        # '\n' + str(i) + ','
        # a[0]  将’19910404'样式的字符串转为'1991-05-05'格式的字符串。为了统一日期格式
        a_date = str(a[0])[0:4] + '-' + str(a[0])[4:6] + '-' + str(a[0])[6:8]
        line = '\n' + str(a_date) + ',' \
               + str(a[1] / 100.0) + ',' \
               + str(a[2] / 100.0) + ',' \
               + str(a[3] / 100.0) + ',' \
               + str(a[4] / 100.0) + ',' \
               + str(a[6]) + ',' \
               + str(Decimal(a[5]).quantize(Decimal("1."), rounding="ROUND_HALF_UP"))
        target_file.write(line)
        begin += 32
        end += 32
    target_file.close()


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


def get_lastest_stocklist():
    """
    使用pytdx从网络获取最新券商列表
    :return:DF格式，股票清单
    """
    import pytdx.hq
    import pytdx.util.best_ip
    print(f"优选通达信行情服务器 也可直接更改为优选好的 {{'ip': '123.125.108.24', 'port': 7709}}")
    # ipinfo = pytdx.util.best_ip.select_best_ip()
    api = pytdx.hq.TdxHq_API()
    # with api.connect(ipinfo['ip'], ipinfo['port']):
    with api.connect('123.125.108.24', 7709):
        data = pd.concat([pd.concat(
            [api.to_df(api.get_security_list(j, i * 1000)).assign(sse='sz' if j == 0 else 'sh') for i in
             range(int(api.get_security_count(j) / 1000) + 1)], axis=0) for j in range(2)], axis=0)
    data = data.reindex(columns=['sse', 'code', 'name', 'pre_close', 'volunit', 'decimal_point'])
    data.sort_values(by=['sse', 'code'], ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    # 这个方法不行 字符串不能运算大于小于，转成int更麻烦
    # df = data.loc[((data['sse'] == 'sh') & ((data['code'] >= '600000') | (data['code'] < '700000'))) | \
    #              ((data['sse'] == 'sz') & ((data['code'] >= '000001') | (data['code'] < '100000'))) | \
    #              ((data['sse'] == 'sz') & ((data['code'] >= '300000') | (data['code'] < '309999')))]
    sh_start_num = data[(data['sse'] == 'sh') & (data['code'] == '600000')].index.tolist()[0]
    sh_end_num = data[(data['sse'] == 'sh') & (data['code'] == '706070')].index.tolist()[0]
    sz00_start_num = data[(data['sse'] == 'sz') & (data['code'] == '000001')].index.tolist()[0]
    sz00_end_num = data[(data['sse'] == 'sz') & (data['code'] == '100303')].index.tolist()[0]
    sz30_start_num = data[(data['sse'] == 'sz') & (data['code'] == '300001')].index.tolist()[0]
    sz30_end_num = data[(data['sse'] == 'sz') & (data['code'] == '395001')].index.tolist()[0]

    df_sh = data.iloc[sh_start_num:sh_end_num]
    df_sz00 = data.iloc[sz00_start_num:sz00_end_num]
    df_sz30 = data.iloc[sz30_start_num:sz30_end_num]

    df = pd.concat([df_sh, df_sz00, df_sz30])
    df.reset_index(drop=True, inplace=True)
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


def list_localTDX_cwfile(ext_name):
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


def readall_local_cwfile():
    """
    将全部财报文件读到df_cw字典里。会占用1G内存，但处理速度比遍历CSV方式快很多
    :return: 字典形式，所有财报内容。
    """
    print(f'开始载入所有财报文件到内存 会占用1G内存 预计需要30秒')
    dict = {}
    cwfile_list = os.listdir(ucfg.tdx['csv_cw'])  # cw目录 生成文件名列表
    starttime_tick = time.time()
    for cwfile in cwfile_list:
        if os.path.getsize(ucfg.tdx['csv_cw'] + os.sep + cwfile) != 0:
            dict[cwfile[4:-4]] = (pd.read_csv(ucfg.tdx['csv_cw'] + os.sep + cwfile,
                                              index_col=0, header=None, encoding='gbk', dtype={1: str}))
    print(f'读取所有财报文件完成 用时{(time.time() - starttime_tick):.2f}秒')
    return dict


def make_fq(code, df_code, df_gbbq, df_cw='', start_date='', end_date='', fqtype='qfq'):
    """
    股票周期数据复权处理函数
    :param code:str格式，具体股票代码
    :param df_code:DF格式，未除权的具体股票日线数据。DF自动生成的数字索引，列定义：date,open,high,low,close,vol,amount
    :param df_gbbq:DF格式，通达信导出的全股票全日期股本变迁数据。DF读取gbbq文件必须加入dtype={'code': str}参数，否则股票代码开头0会忽略
    :param df_cw:DF格式，读入内存的全部财务文件
    :param start_date:可选，要截取的起始日期。默认为空。格式"2020-10-10"
    :param end_date:可选，要截取的截止日期。默认为空。格式"2020-10-10"
    :param fqtype:可选，复权类型。默认前复权。
    :return:复权后的DF格式股票日线数据
    """

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

    # 先进行判断。如果有adj列，且没有NaN值，表示此股票数据已处理完成，无需处理。直接返回。
    # 如果没有‘adj'列，表示没进行过复权处理，当作新股处理
    if 'adj' in df_code.columns.to_list():
        if True in df_code['adj'].isna().to_list():
            first_index = np.where(df_code.isna())[0][0]  # 有NaN值，设为第一个NaN值所在的行
        else:
            return ""
    else:
        first_index = 0

    flag_attach = False  # True=追加数据模式  False=数据全部重新计算
    # 设置新股标志。True=新股，False=旧股。新股跳过追加数据部分的代码。如果财报循环完还是True，说明该股至今无财报，也无法获取流通股本
    flag_newstock = False

    # 提取只有除权除息的行保存到DF df_gbbq
    df_gbbq = df_gbbq.loc[(df_gbbq['类别'] == '除权除息') & (df_gbbq['code'] == code)]
    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04 为了按日期一一对应拼接
    df_gbbq = df_gbbq.assign(date=pd.to_datetime(df_gbbq['权息日'], format='%Y%m%d'))  # 添加date列，设置为datetime64[ns]格式
    df_gbbq.set_index('date', drop=True, inplace=True)  # 设置权息日为索引  (字符串表示的日期 "19910101")
    df_gbbq['category'] = 1.0  # 添加category列
    if len(df_gbbq) > 0:  # =0表示股本变迁中没有该股的除权除息信息。gbbq_lastest_date设置为今天，当作新股处理
        gbbq_lastest_date = df_gbbq.index[-1].strftime('%Y-%m-%d')  # 提取最新的除权除息日
    else:
        gbbq_lastest_date = str(datetime.date.today())
        flag_newstock = True

    # 判断df_code是否已有历史数据，是追加数据还是重新生成。
    # 如果gbbq_lastest_date not in df_code.loc[first_index:, 'date'].to_list()，表示未更新数据中不包括除权除息日
    # 由于前复权的特性，除权后历史数据都要变。因此未更新数据中不包括除权除息日，只需要计算未更新数据。否则日线数据需要全部重新计算
    # 如果'adj'在df_code的列名单里，表示df_code是已复权过的，只需追加新数据，否则日线数据还是需要全部重新计算
    if gbbq_lastest_date not in df_code.loc[first_index:, 'date'].to_list() and not flag_newstock:
        if {'adj'}.issubset(df_code.columns):
            flag_attach = True  # 确定为追加模式
            df_code_original = df_code  # 原始code备份为df_code_original，最后合并
            df_code = df_code.iloc[first_index:]  # 切片df_code，只保留需要处理的行
            df_code.reset_index(drop=True, inplace=True)
            df_code_original.dropna(how='any', inplace=True)  # 丢掉缺失数据的行，之后直接append新数据就行。比merge简单。
            df_code_original['date'] = pd.to_datetime(df_code_original['date'], format='%Y-%m-%d')  # 转为时间格式
            df_code_original.set_index('date', drop=True, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
            # 由于无需搜索财报，所以直接把流通股的值复制过来。后面也直接跳过找财报代码。代码会警告，暂时无法解决
            with pd.option_context('mode.chained_assignment', None):  # 临时屏蔽语句警告
                df_code['流通股'] = df_code_original.at[df_code_original.index[first_index - 1], '流通股']

    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04  为了按日期一一对应拼接
    with pd.option_context('mode.chained_assignment', None):  # 临时屏蔽语句警告
        df_code['date'] = pd.to_datetime(df_code['date'], format='%Y-%m-%d')
    df_code.set_index('date', drop=True, inplace=True)
    df_code.insert(df_code.shape[1], 'if_trade', True)  # 插入if_trade列，赋值True

    # 提取df_gbbq表的category列的值，按日期一一对应，列拼接到bfq_data表。也就是标识出当日是除权除息日的行
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
    # 计算每日复权因子 前复权最近一次股本变迁的复权因子为1
    data['adj'] = (data['preclose'].shift(-1) / data['close']).fillna(1)[::-1].cumprod()
    data['open'] = data['open'] * data['adj']
    data['high'] = data['high'] * data['adj']
    data['low'] = data['low'] * data['adj']
    data['close'] = data['close'] * data['adj']
    # data['preclose'] = data['preclose'] * data['adj']  # 这行没用了
    data = data[data['if_trade']]  # 重建整个表，只保存if_trade列=true的行

    # 抛弃过程处理行，且open值不等于0的行
    data = data.drop(['分红-前流通盘', '配股-后总股本', '配股价-前总股本',
                      '送转股-后流通盘', 'if_trade', 'category', 'preclose'], axis=1)[data['open'] != 0]
    # 复权处理完成

    if not flag_attach:  # 是否追加数据模式。如果是追加数据，则不需要查找财报
        # 如果没有传参进来，就自己读取财务文件，否则用传参的值
        if df_cw == '':
            cw_dict = readall_local_cwfile()
        else:
            cw_dict = df_cw

        # 计算换手率
        # 财报数据公开后，股本才变更。因此有效时间是“当前财报日至未来日期”。故将结束日期设置为2099年。每次财报更新后更新对应的日期时间段
        e_date = '20990101'
        for cw_date in cw_dict:  # 遍历财报字典  cw_date=财报日期  cw_dict[cw_date]=具体的财报内容
            # 如果复权数据表的首行日期>当前要读取的财务报表日期，则表示此财务报表发布时股票还未上市，跳过此次循环。有例外情况：003001
            # (cw_dict[cw_date][1] == code).any() 表示当前股票code在财务DF里有数据
            if data.index[0].strftime('%Y%m%d') <= cw_date and (cw_dict[cw_date][1] == code).any():
                # 获取目前股票所在行的索引值，具有唯一性，所以直接[0]
                code_df_index = cw_dict[cw_date][cw_dict[cw_date][1] == code].index.to_list()[0]
                # DF格式读取的财报，字段与财务说明文件的序号一一对应，如果是CSV读取的，字段需+1
                # print(f'{cwfile_date} 总股本:{cw_dict[cw_date].iat[code_df_index,238]} 流通股本:{cw_dict[cw_date].iat[code_df_index,266]}')
                # 如果流通股值是0，则进行下一次循环
                if cw_dict[cw_date].iat[code_df_index, 266] != '0' or cw_dict[cw_date].iat[code_df_index, 266] != '0.0':
                    data.loc[cw_date:e_date, '流通股'] = float(cw_dict[cw_date].iat[code_df_index, 266])
                    if flag_newstock:
                        flag_newstock = False

    data = data.fillna(method='ffill')  # 向下填充无效值
    data = data.fillna(method='bfill')  # 向上填充无效值  为了弥补开始几行的空值
    data = data.round({'open': 2, 'high': 2, 'low': 2, 'close': 2, })  # 指定列四舍五入
    if not flag_newstock:
        data['流通市值'] = data['流通股'] * data['close']
        data['换手率%'] = data['vol'] / data['流通股'] * 100
        data = data.round({'流通市值': 2, '换手率%': 2, })  # 指定列四舍五入
        if flag_attach:  # 追加模式，则附加最新处理的数据
            data = df_code_original.append(data)

    if len(start_date) == 0 and len(end_date) == 0:
        pass
    elif len(start_date) != 0 and len(end_date) == 0:
        data = data[start_date:]
    elif len(start_date) == 0 and len(end_date) != 0:
        data = data[:end_date]
    elif len(start_date) != 0 and len(end_date) != 0:
        data = data[start_date:end_date]
    data.reset_index(drop=False, inplace=True)  # 重置索引行，数字索引，date列到第一列，保存为str '1991-01-01' 格式
    return data


if __name__ == '__main__':
    stock_code = '003001'
    day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sz/lday', 'sz' + stock_code + '.day', ucfg.tdx['csv_lday'])
    df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})
    df_bfq = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + stock_code + '.csv', index_col=None, encoding='gbk')
    df_qfq = make_fq(stock_code, df_bfq, df_gbbq)
    if len(df_qfq) > 0:
        df_qfq.to_csv(ucfg.tdx['csv_lday'] + os.sep + stock_code + '.csv', index=False, encoding='gbk')
