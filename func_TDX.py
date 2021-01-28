#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
模仿通达信语句的函数库。如MA(C,5) REF(C,1)等样式。
语句简单，只为了和通达信公式看起来一致
作者：wking [http://wkings.net]
"""
import statistics


def REF(value, day):
    """
    引用若干周期前的数据。可以是列表或序列类型
    """
    result = value[~day]
    return result


def MA(value, day):
    """
    返回简单移动平均。可以是列表或序列类型
    """
    result = statistics.mean(value[-day:])
    return result


def HHV(value, day):
    """
    返回最大值
    """
    value = max(value[-day:])
    return value


def LLV(value, day):
    """
    返回最小值
    """
    value = min(value[-day:])
    return value
