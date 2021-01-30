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


@retry(tries=3, delay=3)  # 无限重试装饰性函数，3秒为雪球最佳重试间隔
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
