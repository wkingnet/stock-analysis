import talib
import pandas as pd
from rqalpha.apis import *
from rqalpha import run_func


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    # 在context中保存全局变量
    context.s1 = "000001.XSHE"

    # 设置这个策略当中会用到的参数，在策略中可以随时调用，这个策略使用长短均线，我们在这里设定长线和短线的区间，在调试寻找最佳区间的时候只需要在这里进行数值改动
    context.SHORTPERIOD = 20
    context.LONGPERIOD = 120


# before_trading此函数会在每天策略交易开始前被调用，当天只会被调用一次
def before_trading(context):
    pass


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 因为策略需要用到均线，所以需要读取历史数据
    prices = history_bars(context.s1, context.LONGPERIOD + 1, '1d', 'close')

    # 使用talib计算长短两根均线，均线以array的格式表达
    short_avg = talib.SMA(prices, context.SHORTPERIOD)
    long_avg = talib.SMA(prices, context.LONGPERIOD)

    plot("short avg", short_avg[-1])
    plot("long avg", long_avg[-1])

    # 获取当前投资组合中股票的仓位
    cur_position = get_position(context.s1).quantity
    # 计算现在portfolio中的现金可以购买多少股票
    shares = context.portfolio.cash / bar_dict[context.s1].close

    # 如果短均线从上往下跌破长均线，也就是在目前的bar短线平均值低于长线平均值，而上一个bar的短线平均值高于长线平均值
    if short_avg[-1] - long_avg[-1] < 0 and short_avg[-2] - long_avg[-2] > 0 and cur_position > 0:
        # 进行清仓
        logtmp = "SELL " + str(context.s1) + " 100%"
        logger.info(logtmp)
        order_target_value(context.s1, 0)

    # 如果短均线从下往上突破长均线，为入场信号
    if short_avg[-1] - long_avg[-1] > 0 and short_avg[-2] - long_avg[-2] < 0:
        # 满仓入股
        logtmp = "BUY " + str(context.s1) + " 10%"
        logger.info(logtmp)
        order_percent(context.s1, 0.1)


# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    pass


__config__ = {
    "base": {
        # 回测起始日期
        "start_date": "2019-01-01",
        # 数据源所存储的文件路径
        "data_bundle_path": "C:/Users/king/.rqalpha/bundle/",
        "strategy_file": "huice_rq.py",
        # 目前支持 `1d` (日线回测) 和 `1m` (分钟线回测)，如果要进行分钟线，请注意是否拥有对应的数据源，目前开源版本是不提供对应的数据源的。
        "frequency": "1d",
        "matching_type": "current_bar",
        # 运行类型，`b` 为回测，`p` 为模拟交易, `r` 为实盘交易。
        "run_type": "b",
        # 设置策略可交易品种，目前支持 `stock` (股票账户)、`future` (期货账户)，您也可以自行扩展
        "accounts": {
            # 如果想设置使用某个账户，只需要增加对应的初始资金即可
            "stock": 1000000,
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
            "plot": True,
            "output_file": "rq_result.pkl",
        },
        "sys_progress": {
            "enabled": True,
            "show": True,
        },
    },
}

# 使用 run_func 函数来运行策略
# 此种模式下，您只需要在当前环境下定义策略函数，并传入指定运行的函数，即可运行策略。
# 如果你的函数命名是按照 API 规范来，则可以直接按照以下方式来运行
run_func(**globals())

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
print(result_dict["stock_positions"].tail())