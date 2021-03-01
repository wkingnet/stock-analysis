import backtrader as bt
import pandas as pd
import os
import time
from tqdm import tqdm
import user_config as ucfg
import datetime  # For datetime objects
from backtrader.feeds import PandasData


# datapath = 'E:/code/python资料库/backtrader-master/datas/orcl-1995-2014.txt'
# datapath = 'd:/TDXdata/pickle/000001.pkl'
# # Create a Data Feed
# df = pd.read_pickle(datapath)
# df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
# data = PandasDataEX(dataname=df)

class PandasDataEX(PandasData):
    lines = ('celue_buy', 'celue_sell',)
    params = (('celue_buy', -1), ('celue_sell', -1),)


# Create a Stratey
class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        for data in self.datas:
            print(data._name)
            #print(data.open[0])
            #print(bool(data.celue_buy[0]))
            # self.dataclose = self.datas[0].close
            # self.databuy = self.datas[0].celue_buy
            # self.datasell = self.datas[0].celue_sell
            # print(self.datas[0]._name)

        # To keep track of pending orders
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def start(self):
        self.dtstart = datetime.datetime.now()
        print('Strat Start Time:{}'.format(self.dtstart))

    def prenext(self):
        if len(self.data0) == 1:  # only 1st time
            self.dtprenext = datetime.datetime.now()
            print('Pre-Next Start Time1:{}'.format(self.dtprenext))
            indcalc = (self.dtprenext - self.dtstart).total_seconds()
            print('Time Calculating Indicators: {:.2f}'.format(indcalc))

    def nextstart(self):
        if len(self.data0) == 1:  # there was no prenext
            self.dtprenext = datetime.datetime.now()
            print('Pre-Next Start Time2:{}'.format(self.dtprenext))
            indcalc = (self.dtprenext - self.dtstart).total_seconds()
            print('Time Calculating Indicators: {:.2f}'.format(indcalc))

        self.dtnextstart = datetime.datetime.now()
        print('Next Start Time:             {}'.format(self.dtnextstart))
        warmup = (self.dtnextstart - self.dtprenext).total_seconds()
        print('Strat warm-up period Time:   {:.2f}'.format(warmup))
        nextstart = (self.dtnextstart - self.env.dtcerebro).total_seconds()
        print('Time to Strat Next Logic:    {:.2f}'.format(nextstart))
        self.next()

    def next(self):
        # Simply log the closing price of the series from the reference
        # self.log('Close, %.2f' % self.dataclose[0])

        self.i+=1
        print('next',self.i, datetime.datetime.now())

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        for i, d in enumerate(self.datas):
            dt, dn = self.datetime.date(), d._name  # 获取时间及股票代码
            print(dt,dn)
        # Check if we are in the market
            if not self.getposition(d).size:
                if self.d[d]['buy']:
                    # Keep track of the created order to avoid a 2nd order
                    self.order = self.buy()
            else:
                # Already in the market ... we might sell
                if self.d[d]['sell']:
                    self.order = self.sell()


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    file_list = os.listdir(ucfg.tdx['pickle'])
    #file_list = ['000001.pkl', '600570.pkl', '300496.pkl', '601577.pkl', '600196.pkl', ]
    starttime_tick = time.time()
    tq = tqdm(file_list)
    for filename in tq:
        stock = filename[:-4]
        tq.set_description(filename)
        pklfile = ucfg.tdx['pickle'] + os.sep + filename
        df = pd.read_pickle(pklfile)
        df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
        data = PandasDataEX(dataname=df)
        # Add the Data Feed to Cerebro
        cerebro.adddata(data, name=stock)

    # Set our desired cash start
    cerebro.broker.setcash(1000000.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run(preload=False, exactbars=1)

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
