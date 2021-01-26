#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
模仿通达信语句的函数库。如MA(C,5) REF(C,1)等样式
作者：wking [http://wkings.net]
"""
import statistics


def REF(value, day):
    """
    引用若干周期前的数据。可以是列表或序列类型
    """
    if type(value) in ['list', 'tuple']:  # 列表
        result = value[~day]
    elif 'series' in type(value):  # 序列 或pandas的序列
        result = value.iloc[~day]
    return result


def MA(value, day):
    """
    返回简单移动平均。可以是列表或序列类型
    """
    result = statistics.mean(value[~day:])
    return result


def HHV(value, day):
    """
    返回最大值
    """
    if day == 0:
        value = max(value)
    else:
        value = max(value[~day:])
    return value


def LLV(value, day):
    """
    返回最小值
    """
    if day == 0:
        value = min(value)
    else:
        value = min(value[~day:])
    return value
