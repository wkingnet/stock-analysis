import os
import struct
import datetime


def stock_csv(filepath, name):
    data = []
    with open(filepath, 'rb') as f:
        file_object_path = 'D:/stock/通达信/vipdoc/sh/lday/pythondata/' + name +'.csv'
        file_object = open(file_object_path, 'w+')
        while True:
            stock_date = f.read(4)
            stock_open = f.read(4)
            stock_high = f.read(4)
            stock_low= f.read(4)
            stock_close = f.read(4)
            stock_amount = f.read(4)
            stock_vol = f.read(4)
            stock_reservation = f.read(4)

            # date,open,high,low,close,amount,vol,reservation

            if not stock_date:
                break
            stock_date = struct.unpack("l", stock_date)     # 4字节 如20091229
            stock_open = struct.unpack("l", stock_open)     #开盘价*100
            stock_high = struct.unpack("l", stock_high)     #最高价*100
            stock_low= struct.unpack("l", stock_low)        #最低价*100
            stock_close = struct.unpack("l", stock_close)   #收盘价*100
            stock_amount = struct.unpack("f", stock_amount) #成交额
            stock_vol = struct.unpack("l", stock_vol)       #成交量
            stock_reservation = struct.unpack("l", stock_reservation) #保留值

            date_format = datetime.datetime.strptime(str(stock_date[0]),'%Y%M%d') #格式化日期
            list= date_format.strftime('%Y-%M-%d')+","+str(stock_open[0]/100)+","+str(stock_high[0]/100.0)+","+str(stock_low[0]/100.0)+","+str(stock_close[0]/100.0)+","+str(stock_vol[0])+"\r\n"
            file_object.writelines(list)
        file_object.close()


path = 'D:/stock/通达信/vipdoc/sh/lday/'
listfile = os.listdir('D:/stock/通达信/vipdoc/sh/lday/')
for i in listfile:
	print(listfile)
    stock_csv(path+i, i[:-4])
