#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
读取通达信专业财务数据文件 /vipdoc/cw/gpcw?????.dat
感谢大神们的研究 https://github.com/rainx/pytdx/issues/133

财务文件无需天天更新，上市公司发了季报后财务文件才会更新，因此更新大概率集中在财报季。

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""

import os
import csv
import time
import requests
import datetime
import hashlib
import zipfile
import pandas as pd
import pytdx.reader.gbbq_reader

import func_TDX
import user_config as ucfg


# 变量定义
tdxpath = ucfg.tdx['tdx_path']
starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime_tick = time.time()


# 主程序开始
# 本机专业财务文件和通达信服务器对比，检查更新

# 下载通达信服务器文件校检信息txt
tdx_txt_url = 'http://down.tdx.com.cn:8001/tdxfin/gpcw.txt'
tdx_txt_df = func_TDX.dowload_url(tdx_txt_url)  # 下载gpcw.txt
tdx_txt_df = tdx_txt_df.text.strip().split('\r\n')  # 分割行
tdx_txt_df = [l.strip().split(",") for l in tdx_txt_df]  # 用,分割，二维列表
tdx_txt_df = pd.DataFrame(tdx_txt_df, columns=['filename', 'md5', 'filesize'])  # 转为df格式，好比较

# 检查本机文件是否有缺失
local_zipfile_list = func_TDX.list_local_cwfile('zip')  # 获取本机已有文件
for df_filename in tdx_txt_df['filename'].tolist():
    if df_filename not in local_zipfile_list:
        print(f'{df_filename} 本机不存在 开始下载')
        tdx_zipfile_url = 'http://down.tdx.com.cn:8001/tdxfin/' + df_filename
        download_obj = func_TDX.dowload_url(tdx_zipfile_url)
        local_zipfile_path = ucfg.tdx['tdx_path'] + os.sep + "vipdoc" + os.sep + "cw" + os.sep + df_filename
        with open(local_zipfile_path, 'wb') as fileobj:  # 写入下载的zip文件内容
            fileobj.write(download_obj.content)
        with zipfile.ZipFile(local_zipfile_path, 'r') as zipobj:  # 打开zip对象，释放zip文件。会自动覆盖原文件。
            zipobj.extractall(ucfg.tdx['tdx_path'] + os.sep + "vipdoc" + os.sep + "cw")
        local_datfile_path = local_zipfile_path[:-4] + ".dat"
        df = func_TDX.historyfinancialreader(local_datfile_path)
        csvpath = ucfg.tdx['csv_cw'] + os.sep + df_filename[:-4] + ".csv"
        df.to_csv(csvpath, encoding='gbk', index=True, header=False)  # 会自动覆盖源文件
        print(f'{df_filename} 完成更新 已用{(time.time() - starttime_tick):>5.2f}秒')

# 检查本机文件是否需要更新
local_zipfile_list = func_TDX.list_local_cwfile('zip')  # 获取本机已有文件
for zipfile_filename in local_zipfile_list:
    local_zipfile_path = ucfg.tdx['tdx_path'] + os.sep + "vipdoc" + os.sep + "cw" + os.sep + zipfile_filename
    with open(local_zipfile_path, 'rb') as fobj:  # 读取本机zip文件，计算md5
        file_content = fobj.read()
        file_md5 = hashlib.md5(file_content).hexdigest()
    if file_md5 not in tdx_txt_df['md5'].tolist():  # 本机zip文件的md5与服务器端不一致
        print(f'{zipfile_filename} 需要更新')
        os.remove(local_zipfile_path)  # 删除本机zip文件
        tdx_zipfile_url = 'http://down.tdx.com.cn:8001/tdxfin/' + zipfile_filename
        download_obj = func_TDX.dowload_url(tdx_zipfile_url)  # 下载最新zip文件
        with open(local_zipfile_path, 'wb') as fileobj:   # 写入下载的zip文件内容
            fileobj.write(download_obj.content)
        with zipfile.ZipFile(local_zipfile_path, 'r') as zipobj:  # 打开zip对象，释放zip文件。会自动覆盖原文件。
            zipobj.extractall(ucfg.tdx['tdx_path'] + os.sep + "vipdoc" + os.sep + "cw")
        local_datfile_path = local_zipfile_path[:-4] + ".dat"
        df = func_TDX.historyfinancialreader(local_datfile_path)
        csvpath = ucfg.tdx['csv_cw'] + os.sep + zipfile_filename[:-4] + ".csv"
        df.to_csv(csvpath, encoding='gbk', index=True, header=False)  # 会自动覆盖原文件。
        print(f'{zipfile_filename} 完成更新 已用{(time.time() - starttime_tick):>5.2f}秒')

print(f'专业财务文件检查更新完成')

# 解密通达信股本变迁文件
category = {
    '1': '除权除息', '2': '送配股上市', '3': '非流通股上市', '4': '未知股本变动', '5': '股本变化',
    '6': '增发新股', '7': '股份回购', '8': '增发新股上市', '9': '转配股上市', '10': '可转债上市',
    '11': '扩缩股', '12': '非流通股缩股', '13': '送认购权证', '14': '送认沽权证'}
print(f'解密通达信gbbq股本变迁文件')
filepath = ucfg.tdx['tdx_path'] + '/T0002/hq_cache/gbbq'
df_gbbq = pytdx.reader.gbbq_reader.GbbqReader().get_df(filepath)
df_gbbq.drop(columns=['market'], inplace=True)
df_gbbq.columns = ['code', '权息日', '类别',
                   '分红-前流通盘', '配股价-前总股本', '送转股-后流通盘', '配股-后总股本']
df_gbbq['类别'] = df_gbbq['类别'].astype('object')
df_gbbq['code'] = df_gbbq['code'].astype('object')
for i in range(df_gbbq.shape[0]):
    df_gbbq.iat[i, df_gbbq.columns.get_loc("类别"), ] = category[str(df_gbbq.iat[i, df_gbbq.columns.get_loc("类别")])]
df_gbbq.to_csv(ucfg.tdx['csv_gbbq'] + os.sep + 'gbbq.csv', encoding='gbk', index=False)
# 如果读取，使用下行命令
# df_gbbq = pd.read_csv(ucfg.tdx['csv_cw'] + '/gbbq.csv', encoding='gbk', dtype={'code': 'object'})
print(f'股本变迁解密完成 已用{(time.time() - starttime_tick):>5.2f}秒')
