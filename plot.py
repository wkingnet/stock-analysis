"""
画K线文件，反应策略买入卖出节点。
"""
import os
import sys
import time
import threading
from multiprocessing import Pool, RLock, freeze_support
import numpy as np
import pandas as pd
from tqdm import tqdm
from rich import print as rprint

import CeLue  # 个人策略文件，不分享
import func_TDX
import user_config as ucfg
from pyecharts.charts import Kline, Bar, Grid
from pyecharts.globals import ThemeType
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode

if __name__ == '__main__':
    stock_code = '600362'
    df_stock = pd.read_pickle(ucfg.tdx["pickle"] + os.sep + stock_code + ".pkl")
    # df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
    # df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    # print(df_stock)

    kline = Kline(init_opts=opts.InitOpts(width="100%", height="600px", theme=ThemeType.ESSOS, page_title=stock_code, ))
    # bar = Bar()

    # 做横轴的处理
    datetime = df_stock['date'].astype(str).tolist()
    oclh = []
    # df_stock[['open', 'close', 'low', 'high']].apply(lambda row:oclh.append(row.to_list()))
    for i in range(df_stock.shape[0]):
        oclh.append(df_stock.loc[i, ['open', 'close', 'low', 'high']].to_list())

    # 生成买点卖点区域标示坐标点
    df_celue = df_stock.loc[df_stock['celue_buy'] | df_stock['celue_sell']]  # 提取买卖点列
    yAxis_max = df_stock['high'].max()
    markareadata = []
    temp = []
    # k是range索引，对应图形第几个点,v是K行的内容，字典类型
    for k, v in df_celue.iterrows():
        temp.append(
            {
                "xAxis": k,
                # "yAxis": yAxis_max if v['celue_sell'] else 0,  # buy点是0，sell点是最大值 填了y坐标会导致图形放大后区域消失
            }
        )
        # 如果temp列表数量到达2，表示起点xy坐标、终点xy坐标生成完毕。添加到markareadata，清空temp重新开始
        if len(temp) == 2:
            # 给第2组xy坐标字典添加'itemStyle': {'color': '#14b143'}键值对。
            # df_celue.at[temp[1]['xAxis'], 'close']为读取对应索引的收盘价。
            # 第二组坐标收盘价和第一组坐标收盘价比较，大于则区域颜色是红色表示盈利，小于则绿色亏损
            temp[1]["itemStyle"] = {'color': "#ef232a" if df_celue.at[temp[1]['xAxis'], 'close'] > df_celue.at[
                temp[0]['xAxis'], 'close'] else "#14b143"}
            markareadata.append(temp)
            # rprint(markareadata)
            temp = []

    vol = df_stock['vol'].tolist()
    # print(oclh)
    kline.add_xaxis(datetime)
    kline.add_yaxis(stock_code, oclh, itemstyle_opts=opts.ItemStyleOpts(
        color="#ef232a",
        color0="#14b143",
        border_color="#ef232a",
        border_color0="#14b143", ),
                    # markpoint_opts=opts.MarkPointOpts(
                    #     data=[
                    #         opts.MarkPointItem(type_="max", name="最大值"),
                    #         opts.MarkPointItem(type_="min", name="最小值"),
                    #     ]
                    # )
                    )
    kline.set_series_opts(
        markarea_opts=opts.MarkAreaOpts(is_silent=True, data=markareadata,
                                        itemstyle_opts=opts.ItemStyleOpts(opacity=0.5,
                                                                          )
                                        )
    )
    kline.set_global_opts(
        xaxis_opts=opts.AxisOpts(is_scale=True),
        yaxis_opts=opts.AxisOpts(
            is_scale=True,
            splitline_opts=opts.SplitLineOpts(is_show=True),
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="line"),
        datazoom_opts=[
            opts.DataZoomOpts(type_="inside", range_start=-100),
            opts.DataZoomOpts(pos_bottom="0%"),
        ],
        # title_opts=opts.TitleOpts(title=stock_code),
    )

    grid_chart = Grid(init_opts=opts.InitOpts(width="100%", height="950px",
                                              theme=ThemeType.ESSOS,
                                              page_title=stock_code, ))
    grid_chart.add_js_funcs("var areaData={}".format(markareadata))
    grid_chart.add_js_funcs("console.log('hello world')")
    grid_chart.add(
        kline,
        grid_opts=opts.GridOpts(
            pos_left="3%", pos_right="1%", height="85%"
        ),
    )
    grid_chart.render('pyecharts.html')
