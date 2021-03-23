# coding: utf-8
# see https://github.com/rainx/pytdx/issues/38 IP寻优的简单办法
# by yutianst

import os
import sys
import datetime
import pandas as pd
from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
from rich import print
import user_config as ucfg

stock_ip = [
    {'ip': '114.80.149.19', 'port': 7709},
    {'ip': '114.80.149.22', 'port': 7709},
    {'ip': '114.80.149.84', 'port': 7709},
    {'ip': '114.80.80.222', 'port': 7709},
    {'ip': '115.238.56.198', 'port': 7709},
    {'ip': '115.238.90.165', 'port': 7709},
    {'ip': '117.184.140.156', 'port': 7709},
    {'ip': '119.147.164.60', 'port': 7709},
    {'ip': '123.125.108.23', 'port': 7709},
    {'ip': '123.125.108.24', 'port': 7709},
    {'ip': '124.160.88.183', 'port': 7709},
    {'ip': '180.153.18.17', 'port': 7709},
    {'ip': '180.153.18.170', 'port': 7709},
    {'ip': '180.153.18.171', 'port': 7709},
    {'ip': '180.153.39.51', 'port': 7709},
    {'ip': '218.108.47.69', 'port': 7709},
    {'ip': '218.108.50.178', 'port': 7709},
    {'ip': '218.108.98.244', 'port': 7709},
    {'ip': '218.75.126.9', 'port': 7709},
    {'ip': '218.9.148.108', 'port': 7709},
    {'ip': '221.194.181.176', 'port': 7709},
    {'ip': '59.173.18.69', 'port': 7709},
    {'ip': '60.12.136.250', 'port': 7709},
    {'ip': '60.191.117.167', 'port': 7709},
    {'ip': '61.135.142.88', 'port': 7709},
    {'ip': '61.152.107.168', 'port': 7721},
    {'ip': '61.152.249.56', 'port': 7709},
    {'ip': '61.153.144.179', 'port': 7709},
    {'ip': '61.153.209.138', 'port': 7709},
    {'ip': '61.153.209.139', 'port': 7709},
    {'ip': 'hq.cjis.cn', 'port': 7709},
    {'ip': 'jstdx.gtjas.com', 'port': 7709},
    {'ip': 'shtdx.gtjas.com', 'port': 7709},
    {'ip': '180.153.18.170', 'port': 7709},
    {'ip': '180.153.18.171', 'port': 7709},
    {'ip': '180.153.18.172', 'port': 7709},
    {'ip': '202.108.253.131', 'port': 7709},
    {'ip': '60.191.117.167', 'port': 7709},
    {'ip': '115.238.90.165', 'port': 7709},
    {'ip': '218.108.98.244', 'port': 7709},
    {'ip': '123.125.108.23', 'port': 7709},
    {'ip': '123.125.108.24', 'port': 7709},
    {'ip': '58.67.221.146', 'port': 7709},
    {'ip': '103.24.178.242', 'port': 7709},
    {'ip': '103.24.178.242', 'port': 7709},
    {'ip': '218.6.170.55', 'port': 7709}, ]

future_ip = [{'ip': '106.14.95.149', 'port': 7727, 'name': '扩展市场上海双线'},
             {'ip': '112.74.214.43', 'port': 7727, 'name': '扩展市场深圳双线1'},
             {'ip': '119.147.86.171', 'port': 7727, 'name': '扩展市场深圳主站'},
             {'ip': '119.97.185.5', 'port': 7727, 'name': '扩展市场武汉主站1'},
             {'ip': '120.24.0.77', 'port': 7727, 'name': '扩展市场深圳双线2'},
             {'ip': '124.74.236.94', 'port': 7721},
             {'ip': '202.103.36.71', 'port': 443, 'name': '扩展市场武汉主站2'},
             {'ip': '47.92.127.181', 'port': 7727, 'name': '扩展市场北京主站'},
             {'ip': '59.175.238.38', 'port': 7727, 'name': '扩展市场武汉主站3'},
             {'ip': '61.152.107.141', 'port': 7727, 'name': '扩展市场上海主站1'},
             {'ip': '61.152.107.171', 'port': 7727, 'name': '扩展市场上海主站2'},
             {'ip': '119.147.86.171', 'port': 7721, 'name': '扩展市场深圳主站'},
             {'ip': '47.107.75.159', 'port': 7727, 'name': '扩展市场深圳双线3'}]


def ping(ip, port=7709, type_='stock'):
    api = TdxHq_API()
    apix = TdxExHq_API()
    __time1 = datetime.datetime.now()
    try:
        if type_ in ['stock']:
            with api.connect(ip, port, time_out=0.7):
                res = api.get_security_list(0, 1)
                # print(len(res))
                if res is not None:
                    if len(res) > 800:
                        print('GOOD RESPONSE {}'.format(ip))
                        return datetime.datetime.now() - __time1
                    else:
                        print('BAD RESPONSE {}'.format(ip))
                        return datetime.timedelta(9, 9, 0)
                else:

                    print('BAD RESPONSE {}'.format(ip))
                    return datetime.timedelta(9, 9, 0)
        elif type_ in ['future']:
            with apix.connect(ip, port, time_out=0.7):
                res = apix.get_instrument_count()
                if res is not None:
                    if res > 20000:
                        print('GOOD RESPONSE {}'.format(ip))
                        return datetime.datetime.now() - __time1
                    else:
                        print('️Bad FUTUREIP REPSONSE {}'.format(ip))
                        return datetime.timedelta(9, 9, 0)
                else:
                    print('️Bad FUTUREIP REPSONSE {}'.format(ip))
                    return datetime.timedelta(9, 9, 0)
    except Exception as e:
        if isinstance(e, TypeError):
            print(e)
            print('Tushare内置的pytdx版本和最新的pytdx 版本不同, 请重新安装pytdx以解决此问题')
            print('pip uninstall pytdx')
            print('pip install pytdx')

        else:
            print('BAD RESPONSE {}'.format(ip))
        return datetime.timedelta(9, 9, 0)


def select_best_ip(_type='stock'):
    """目前这里给的是单线程的选优, 如果需要多进程的选优/ 最优ip缓存 可以参考
    https://github.com/QUANTAXIS/QUANTAXIS/blob/master/QUANTAXIS/QAFetch/QATdx.py#L106


    Keyword Arguments:
        _type {str} -- [description] (default: {'stock'})
    
    Returns:
        [type] -- [description]
    """
    best_ip = {
        'stock': {
            'ip': None, 'port': None
        },
        'future': {
            'ip': None, 'port': None
        }
    }
    ip_list = stock_ip if _type == 'stock' else future_ip

    data = [ping(x['ip'], x['port'], _type) for x in ip_list]
    results = []
    for i in range(len(data)):
        # 删除ping不通的数据
        if data[i] < datetime.timedelta(0, 9, 0):
            results.append((data[i], ip_list[i]))

    # 按照ping值从小大大排序
    results = [x[1] for x in sorted(results, key=lambda x: x[0])]


    # # 测试服务器在线行情返回股票数
    # stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_lday'])]
    # stocklist_tmp = []
    # for stock in stocklist:  # 构造get_security_quotes所需的元组参数
    #     if stock[:1] == '6':
    #         stocklist_tmp.append(tuple([1, stock]))
    #     elif stock[:1] == '0' or stock[:1] == '3':
    #         stocklist_tmp.append(tuple([0, stock]))
    # stocklist = stocklist_tmp
    # api = TdxHq_API()
    # for r_dict in results[:10]:
    #     df = pd.DataFrame()
    #     if api.connect(r_dict['ip'], r_dict['port']):
    #         k = 0
    #         for v in stocklist:
    #             if k > 0 and k % 80 == 0:
    #                 data = api.to_df(api.get_security_quotes(stocklist[k - 80:k]))
    #                 df = pd.concat([df, data], axis=0, ignore_index=True)
    #             elif k == len(stocklist) - 1:  # 循环到最后，少于10个构成一组
    #                 data = api.to_df(api.get_security_quotes(stocklist[k - (k % 80):k + 1]))
    #                 df = pd.concat([df, data], axis=0, ignore_index=True)
    #             k = k + 1
    #     api.disconnect()
    #     df.dropna(how='all', inplace=True)
    #     print(f"服务器\t{r_dict['ip']}\tget_security_quotes函数每80股票一组，可获取 {len(df)} 股票")

    return results[0]


if __name__ == '__main__':
    ip = select_best_ip('stock')
    print(ip)
    # ip = select_best_ip('future')
    # print(ip)
