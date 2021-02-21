import backtrader as bt
import pandas as pd
import datetime  # For datetime objects
from backtrader.feeds import PandasData
# datapath = 'E:/code/python资料库/backtrader-master/datas/orcl-1995-2014.txt'
# datapath = 'd:/TDXdata/pickle/000001.pkl'
# # Create a Data Feed
# df = pd.read_pickle(datapath)
# df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
# data = PandasDataEX(dataname=df)

class PandasDataEX(PandasData):
    # Add a 'pe' line to the inherited ones from the base class
    lines = ('celue2',)

    # openinterest in GenericCSVData has index 7 ... add 1
    # add the parameter to the parameters inherited from the base class
    params = (('celue2', -1), )



# Create a Stratey
class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        if self.dataclose[0] < self.dataclose[-1]:
            # current close less than previous close

            if self.dataclose[-1] < self.dataclose[-2]:
                # previous close less than the previous close

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                self.buy()

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    datapath = 'd:/TDXdata/pickle/000001.pkl'
    # Create a Data Feed
    df = pd.read_pickle(datapath)
    df.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    data = PandasDataEX(dataname=df)

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(1000000.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
