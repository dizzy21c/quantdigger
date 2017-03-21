#!/usr/bin/python
# encoding: utf-8

"""
@author:
contact:
@file:test.py
@time: 17-2-27 下午10:25
"""

from quantdigger.datasource import datautil
from datetime import datetime
import pandas as pd
from quantdigger import ConfigUtil
from quantdigger.datasource import ds_impl
from xpinyin import Pinyin
import types

# t='2018-12-12 12:00:00'
s='2011-10-10 15:00:00'
timeTuple = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

# print datautil.encode2id('1.DAY', timeTuple)[1]

# print datetime.now()

code='002000'

print code[0:4]

print "%6.2f" % 23.33333

# csv_ds = ds_impl.csv_source.CsvSource('data')
# aaa=csv_ds.get_contracts()
# for a in aaa.index:
#     print a
# for a in aaa:
#     print 'test'
    # print aaa[a]
# file = open('calc/out-' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S'), 'a+')
# dict={'a':'va', 'b':'vb'}
# for k, v in dict.items():
#     # print "aaa".format("%s %s "%(k, v))
#     file.write("\"%s\":\"%s\"," % (k, v))
#
# file.close()

# print 1 % 5
#
# print type(str(1))

# print min(1.1,2.2) ,  max(1.1,2.2)