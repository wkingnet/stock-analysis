import os
import time
import talib
import pandas as pd
import user_config as ucfg
from rqalpha.apis import *
from rqalpha import run_func
from tqdm import tqdm
from rich import print as rprint

# 回测变量定义
start_date = "2016-01-01"  # 回测起始日期
stock_money = 10000000  # 股票账户初始资金
xiadan_percent = 0.1  # 设定买入总资产百分比的股票份额
xiadan_target_value = 100000  # 设定具体股票买入持有总金额
# 下单模式 买入总资产百分比的股票份额，或买入持有总金额的股票， 'order_percent' or 'order_target_value'
order_type = 'order_target_value'


def update_stockcode(stockcode):
    if stockcode[0:1] == '6':
        stockcode = stockcode + ".XSHG"
    else:
        stockcode = stockcode + ".XSHE"
    return stockcode


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    # 在context中保存全局变量
    context.percent = xiadan_percent  # 设定买入比例
    context.target_value = xiadan_target_value  # 设定具体股票总买入市值
    context.order_type = order_type  # 下单模式
    # context.stockslist = []
    # context.df = {}
    # file_list = os.listdir(ucfg.tdx['pickle'])
    # tq = tqdm(file_list)
    # for filename in tq:
    #     if filename[0:1] == '6':
    #         stock = filename[:-4] + ".XSHG"
    #     else:
    #         stock = filename[:-4] + ".XSHE"
    #     tq.set_description(stock)
    #     pklfile = ucfg.tdx['pickle'] + os.sep + filename
    #     df = pd.read_pickle(pklfile)
    #     df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    #     context.df[stock] = df
    #     context.stockslist.append(stock)

    df_celue = pd.read_csv(ucfg.tdx['csv_gbbq'] + os.sep + 'celue汇总.csv',
                           index_col=0, encoding='gbk', dtype={'code': str})
    df_celue['code'] = df_celue['code'].apply(lambda x: update_stockcode(x))  # 升级股票代码，匹配rqalpha
    df_celue['date'] = pd.to_datetime(df_celue['date'], format='%Y-%m-%d')  # 转为时间格式
    df_celue.set_index('date', drop=False, inplace=True)  # 时间为索引
    context.df_celue = df_celue


# before_trading此函数会在每天策略交易开始前被调用，当天只会被调用一次
def before_trading(context):
    current_date = context.now.strftime('%Y-%m-%d')
    # 提取当天的df_celue
    if current_date in context.df_celue.index:
        context.df_today = context.df_celue.loc[[current_date]]
    else:
        context.df_today = None


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    if context.df_today is not None:
        for index, row in context.df_today.iterrows():
            # logger.info(index, row)

            # 获取当前投资组合中具体股票的仓位
            cur_position = get_position(row['code']).quantity

            # 卖出股票
            if row['celue_sell'] and cur_position > 0:
                order_target_value(row['code'], 0)
                logger.info(f"SELL {row['code']} -> 0")

            # 买入股票
            if row['celue_buy'] and cur_position == 0:
                if context.order_type == 'order_percent':
                    # 买入/卖出证券以自动调整该证券的仓位到占有一个目标价值。
                    # 加仓时，percent 代表证券已有持仓的价值加上即将花费的现金（包含税费）的总值占当前投资组合总价值的比例。
                    # 减仓时，percent 代表证券将被调整到的目标价至占当前投资组合总价值的比例。
                    order_percent(row['code'], context.percent)
                    logger.info(f"BUY {row['code']} -> {context.percent}")

                elif context.order_type == 'order_target_value':
                    # 买入 / 卖出并且自动调整该证券的仓位到一个目标价值。
                    # 加仓时，cash_amount代表现有持仓的价值加上即将花费（包含税费）的现金的总价值。
                    # 减仓时，cash_amount代表调整仓位的目标价至。
                    # 需要注意，如果资金不足，该API将不会创建发送订单。
                    order_target_value(row['code'], context.target_value)
                    logger.info(f"BUY {row['code']} -> {context.target_value}")


# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    #rprint(get_open_orders())
    string = f'净值{context.portfolio.total_value:>.2f} '
    string += f'可用{context.portfolio.cash:>.2f} '
    string += f'市值{context.portfolio.market_value:>.2f} '
    # string += f'收益{context.portfolio.total_returns:>.2%} '
    string += f'持股{len(context.portfolio.positions):>d} '
    logger.info(string)


__config__ = {
    "base": {
        # 回测起始日期
        "start_date": start_date,
        # 数据源所存储的文件路径
        "data_bundle_path": "C:/Users/king/.rqalpha/bundle/",
        "strategy_file": "huice_rq.py",
        # 目前支持 `1d` (日线回测) 和 `1m` (分钟线回测)，如果要进行分钟线，请注意是否拥有对应的数据源，目前开源版本是不提供对应的数据源的。
        "frequency": "1d",
        # 启用的回测引擎，目前支持 current_bar (当前Bar收盘价撮合) 和 next_bar (下一个Bar开盘价撮合)
        "matching_type": "current_bar",
        # 运行类型，`b` 为回测，`p` 为模拟交易, `r` 为实盘交易。
        "run_type": "b",
        # 设置策略可交易品种，目前支持 `stock` (股票账户)、`future` (期货账户)，您也可以自行扩展
        "accounts": {
            # 如果想设置使用某个账户，只需要增加对应的初始资金即可
            "stock": stock_money,
        },
        # 设置初始仓位
        "init_positions": {}
    },
    "extra": {
        # 选择日期的输出等级，有 `verbose` | `info` | `warning` | `error` 等选项，您可以通过设置 `verbose` 来查看最详细的日志，
        "log_level": "info",
    },

    "mod": {
        "sys_analyser": {
            "enabled": True,
            "benchmark": "000300.XSHG",
            # "plot": True,
            'plot_save_file': "rq_result.png",
            "output_file": "rq_result.pkl",
            # "report_save_path": "rq_result.csv",
        },
        # 策略运行过程中显示的进度条的控制
        "sys_progress": {
            "enabled": False,
            "show": True,
        },
    },
}

start_time = f'程序开始时间：{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}'

# 使用 run_func 函数来运行策略
# 此种模式下，您只需要在当前环境下定义策略函数，并传入指定运行的函数，即可运行策略。
# 如果你的函数命名是按照 API 规范来，则可以直接按照以下方式来运行
run_func(**globals())
end_time = f'程序结束时间：{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}'

# RQAlpha可以输出一个 pickle 文件，里面为一个 dict 。keys 包括
# summary 回测摘要
# stock_portfolios 股票帐号的市值
# future_portfolios 期货帐号的市值
# total_portfolios 总账号的的市值
# benchmark_portfolios 基准帐号的市值
# stock_positions 股票持仓
# future_positions 期货仓位
# benchmark_positions 基准仓位
# trades 交易详情（交割单）
# plots 调用plot画图时，记录的值
result_dict = pd.read_pickle("rq_result.pkl")
rprint(result_dict["summary"])
rprint(start_time)
rprint(end_time)
rprint(
    f"回测起点 {result_dict['summary']['start_date']}"
    f"\n回测终点 {result_dict['summary']['end_date']}"
    f"\n回测收益 {result_dict['summary']['total_returns']:>.2%}\t年化收益 {result_dict['summary']['annualized_returns']:>.2%}"
    f"\t基准收益 {result_dict['summary']['benchmark_total_returns']:>.2%}\t基准年化 {result_dict['summary']['benchmark_annualized_returns']:>.2%}"
    f"\t最大回撤 {result_dict['summary']['max_drawdown']:>.2%}"
    f"\n打开程序文件夹下的rq_result.png查看收益走势图")
