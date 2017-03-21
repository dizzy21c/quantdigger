# -*- coding: utf-8 -*-
import matplotlib

import talib
import math
import numpy as np
matplotlib.use('TkAgg')

import matplotlib.pyplot as plt
import pandas as pd
from quantdigger.technicals import *

# 创建画布
fig, ax = plt.subplots()
# 加载数据
price_data = pd.read_csv("./data/1DAY/SH/600718.csv",
                         index_col=0, parse_dates=True)
# 创建平均线
ma10 = MA(price_data.close, 10, 'MA10', 'y', 2)
ma20 = MA(price_data.close, 60, 'MA10', 'b', 2)
# 绘制指标
# ma10.plot(ax)
# ma20.plot(ax)


# macd=BOLL(price_data.close, 20)

# short=12
# long=26
# mid=9
# dif=EMA(price_data.close, short)
# dif2=EMA(price_data.close, long)
#
# a3=talib.ADD(dif.values, dif2.values)
# a4=talib.SUB(dif.values, dif2.values)
# dea=EMA(a4, mid)
# macd=(dif-dea)*2
#
# # a=[NaN, 1.0,3.0,5.0]
# # b=[nan, 2.0,4.0,6.0]
#
# print np.asarray(a)
# n=talib.ADD(np.asarray(a),np.asarray(b))
#
# print n
#
# n=talib.SUB(np.asarray(a),np.asarray(b))
#
# print n
macd=MACD(price_data.close)

print macd.values['dif'][ len(macd.data) - 10 : len(macd.data) ]
print macd.values['dea'][len(macd.data) - 10 : len(macd.data) ]
print macd.values['macd'][ len(macd.data) - 10 : len(macd.data) ]
/home/zjx/Desktop/home1/baiduyun/bdy/python/fin/第3周
# macd.plot(ax)

# plt.plot(macd)

# plt.box(on)
# plt.boxplot(h)

macd.plot(ax)
#
# for d in macd.values['hist']:
#     print d

plt.show()