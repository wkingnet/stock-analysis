#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import time
from struct import unpack
 
source = 'd:/stock/通达信'   #指定通达信目录
target = 'd:/日线数据'  #指定数据保存目录

# 将通达信的日线文件转换成CSV格式函数
def day2csv(source_dir, file_name, target_dir):
    # 以二进制方式打开源文件
    source_file = open(source_dir + os.sep + file_name, 'rb')
    buf = source_file.read()
    source_file.close()
 
    # 打开目标文件，后缀名为CSV
    target_file = open(target_dir + os.sep + file_name[2:-4] + '.csv', 'w')
    buf_size = len(buf)
    rec_count = buf_size // 32
    begin = 0
    end = 32
    header = str('date') + ', ' + str('open') + ', ' + str('high') + ', ' + str('low') + ', ' \
        + str('close') + ', ' + str('amount') + ', ' + str('vol') + ', ' + str('str07') + '\n'
    target_file.write(header)
    for i in range(rec_count):
        # 将字节流转换成Python数据格式
        # I: unsigned int
        # f: float
        a = unpack('IIIIIfII', buf[begin:end])
        line = str(a[0]) + ', ' + str(a[1] / 100.0) + ', ' + str(a[2] / 100.0) + ', ' \
            + str(a[3] / 100.0) + ', ' + str(a[4] / 100.0) + ', ' + str(a[5] / 10.0) + ', ' \
            + str(a[6]) + ', ' + str(a[7]) + ', ' + '\n'
        target_file.write(line)
        begin += 32
        end += 32
    target_file.close()

#判断目录和文件是否存在，存在则直接删除
if os.path.exists(target):
    choose = input("文件已存在，是否删除？ y/n ")
    if choose == 'y':
        for root, dirs, files in os.walk(target, topdown=False):
            for name in files:
                os.remove(os.path.join(root,name))
            for name in dirs:
                os.rmdir(os.path.join(root,name))
else:
    os.mkdir(target)


#处理沪市股票
file_list = os.listdir(source + '/vipdoc/sh/lday')
begintime = time.time()
for f in file_list:
    if f[0:3] == 'sh6' or f[0:8] == 'sh999999': #处理沪市sh6开头和sh999999(上证指数)文件，否则跳过此次循环
        day2csv(source + '/vipdoc/sh/lday', f, target)
        print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
    else:
        continue
print('沪市处理完毕，用时' + str(time.time()-begintime))
os.rename(target + '/999999.csv', target + '/上证指数.csv') 

#处理深市股票
file_list = os.listdir(source + '/vipdoc/sz/lday')
begintime = time.time()
for f in file_list:
    if f[0:4] == 'sz00' or f[0:4] == 'sz30':    #处理深市sh00开头和创业板sh30文件，否则跳过此次循环
        day2csv(source + '/vipdoc/sz/lday', f, target)
        print(time.strftime("[%H:%M:%S] 处理 ", time.localtime()) + f)
    else:
        continue
print('深市处理完毕，用时' + str(time.time()-begintime))