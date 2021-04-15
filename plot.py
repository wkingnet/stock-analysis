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
from rich import print as print

import CeLue  # 个人策略文件，不分享
import func_TDX
import user_config as ucfg
from pyecharts.charts import Kline, Bar, Grid
from pyecharts.globals import ThemeType
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode

def markareadata(df_stock):
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
    return markareadata

def marklinedata(df_stock):
    # 生成趋势线数据

    import math
    from func_TDX import SMA, BARSLASTCOUNT

    """
    与下面的通达信公式效果完全一致：
    现价:CONST(C),COLORLIGRAY,DOTLINE;
    MAA10:=MA(CLOSE,55);
    高突:=BARSLASTCOUNT(L>MAA10)=9;
    低突:=BARSLASTCOUNT(H<MAA10)=9;
    高突破:=高突 ;
    低突破:=低突 ;
    距上次高位置:=BARSLAST(高突破),NODRAW;
    距上次低位置:=BARSLAST(低突破),NODRAW;
    高过滤:=(高突破 AND REF(距上次高位置,1)>REF(距上次低位置,1));
    低过滤:=(低突破 AND REF(距上次低位置,1)>REF(距上次高位置,1));
    高0:=BACKSET(高过滤,10);
    低0:=BACKSET(低过滤,10);
    高1:=CROSS(高0,0.5);
    低1:=CROSS(低0,0.5);
    距上高位:=BARSLAST(高1),NODRAW;
    距上低位:=BARSLAST(低1),NODRAW;
    低点:=IF(距上高位 > 距上低位, LLV(L,距上低位+1)=L,0);
    低:=FILTERX(低点 AND 距上高位>距上低位,距上低位+1);
    高点:=IF(距上高位 < 距上低位, HHV(H,距上高位+1)=H,0);
    高:=FILTERX(高点 AND 距上低位>距上高位 ,距上高位+1);
    NOTEXT上涨线:DRAWLINE(低 AND BARSLAST(高)>20,L,高 AND BARSLAST(低)>20,H,0),COLORRED,LINETHICK2;
    NOTEXT下跌线:DRAWLINE(高 AND BARSLAST(低)>20,H,低 AND BARSLAST(高)>20,L,0),COLORGREEN,LINETHICK2;
    """

    df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
    df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并

    H = df_stock['high']
    L = df_stock['low']
    C = df_stock['close']

    TJ04_均线 = SMA(C, 55)
    TJ04_高突破 = BARSLASTCOUNT(L > TJ04_均线) == 9
    TJ04_低突破 = BARSLASTCOUNT(H < TJ04_均线) == 9
    TJ04_高突破 = pd.DataFrame(TJ04_高突破.loc[TJ04_高突破 == True], columns=["高突破"])
    TJ04_低突破 = pd.DataFrame(TJ04_低突破.loc[TJ04_低突破 == True], columns=["低突破"])
    TJ04_过滤 = pd.concat([TJ04_高突破, TJ04_低突破]).fillna(value=False).sort_index()
    del TJ04_均线, TJ04_高突破, TJ04_低突破
    高, 低 = 0, 0
    # 过滤高低突破信号循环逻辑：日期由远及近，高低突破信号依次取值，保留各自最相近的一个
    for index, row in TJ04_过滤[:].iterrows():
        if row['高突破'] and 高 == 1:
            TJ04_过滤.drop(index=index, inplace=True)
        elif row['低突破'] and 低 == 1:
            TJ04_过滤.drop(index=index, inplace=True)
        elif row['高突破'] and 高 == 0:
            高 = 1
            低 = 0
        elif row['低突破'] and 低 == 0:
            高 = 0
            低 = 1

    # 寻找阶段高低点
    TJ04_过滤.reset_index(drop=False, inplace=True)
    TJ04_高低点 = pd.DataFrame()
    last_day = None
    for index, row in TJ04_过滤.iterrows():
        if index == 0:
            last_day = row['date']
            continue
        elif row['高突破']:
            s_date = last_day  # 日期区间起点
            e_date = row['date']  # 日期区间终点
            low_date = L.loc[s_date:e_date].idxmin()  # 低点日
            low_value = L.loc[s_date:e_date].min()  # 低点数值
            last_day = low_date
            df_temp = pd.Series(data={'低点价格': low_value,
                                      '低点日期': low_date,
                                      },
                                name=index,
                                )
        elif row['低突破']:
            s_date = last_day  # 日期区间起点
            e_date = row['date']  # 日期区间终点
            high_date = H.loc[s_date:e_date].idxmax()  # 高点日
            high_value = H.loc[s_date:e_date].max()  # 高点数值
            last_day = high_date
            df_temp = pd.Series(data={'高点价格': high_value,
                                      '高点日期': high_date,
                                      },
                                name=index,
                                )
        TJ04_高低点 = TJ04_高低点.append(df_temp)
    TJ04_高低点.reset_index(drop=True, inplace=True)

    # 转换为pyecharts所需数据格式
    marklinedata = []
    temp = []
    """
    x坐标是日期对应的整数序号，y坐标是价格
    所需数据格式: [[{'xAxis': 起点x坐标, 'yAxis': 起点y坐标, 'value': 线长}, {'xAxis': 终点x坐标, 'yAxis': 终点y坐标}]，
                 [{'xAxis': 起点x坐标, 'yAxis': 起点y坐标, 'value': 线长}, {'xAxis': 终点x坐标, 'yAxis': 终点y坐标}],
                ]
    """
    last_day, last_value = 0, 0
    for index, row in TJ04_高低点.iterrows():
        if index == 0:
            if pd.isna(row['低点价格']):  # True=高点是有效数值 False=低点是有效数值
                last_day = row['高点日期']
                last_value = row['高点价格']
            else:
                last_day = row['低点日期']
                last_value = row['低点价格']
            continue
        elif pd.isna(row['低点价格']):  # True=高点是有效数值 False=低点是有效数值
            # 上涨起点坐标
            temp.append(
                {
                    "xAxis": df_stock.index.get_loc(last_day),
                    "yAxis": last_value,
                },
            )
            # 上涨终点坐标
            temp.append(
                {
                    "xAxis": df_stock.index.get_loc(row['高点日期']),
                    "yAxis": row['高点价格'],
                    "lineStyle": {'color': "#ef232a"},
                },
            )
            last_day = row['高点日期']
            last_value = row['高点价格']
        else:
            # 下跌起点坐标
            temp.append(
                {
                    "xAxis": df_stock.index.get_loc(last_day),
                    "yAxis": last_value,
                },
            )
            # 下跌终点坐标
            temp.append(
                {
                    "xAxis": df_stock.index.get_loc(row['低点日期']),
                    "yAxis": row['低点价格'],
                    "lineStyle": {'color': "#14b143"},
                },
            )
            last_day = row['低点日期']
            last_value = row['低点价格']
        marklinedata.append(temp)
        temp = []
    # print(marklinedata)
    return marklinedata

if __name__ == '__main__':

    stock_code = "300496"

    try:
        if len(sys.argv[1:][0]) == 6:
            stock_code = sys.argv[1:][0]
        else:
            raise ValueError("参数非股票代码，需正确输入参数 格式: python celue.py 000001")
    except IndexError as error:
        print("没有获取到参数，需手动附加参数 格式: python celue.py 000001")
        print(f"使用代码内置的股票代码 {stock_code}")

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
                    # ),
                    markline_opts=opts.MarkLineOpts(
                        label_opts=opts.LabelOpts(
                            position="middle", color="blue", font_size=15
                        ),
                        data=marklinedata(df_stock.copy()),
                        symbol=["none", "none"],
                        linestyle_opts=opts.LineStyleOpts(
                            width=2,
                            type_="solid",
                        ),
                    ),
                    )
    kline.set_series_opts(
        markarea_opts=opts.MarkAreaOpts(is_silent=True, data=markareadata(df_stock),
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
    grid_chart.add_js_funcs("var areaData={}".format(markareadata(df_stock)))
    grid_chart.add_js_funcs("console.log('hello world')")
    grid_chart.add(
        kline,
        grid_opts=opts.GridOpts(
            pos_left="3%", pos_right="1%", height="85%"
        ),
    )
    grid_chart.render('plot.html')
    print(f'{stock_code} 绘图完成，打开plot.html文件查看结果，程序结束')
