#!/usr/bin/python
# encoding: utf-8

"""
@author:
contact:
@file:import_csv.py
@time: 17-2-20 下午5:32
"""
import datetime
import os
import time
import pandas as pd
from quantdigger.errors import ArgumentError
from datetime import datetime, timedelta
from threading import Thread, current_thread, Lock

import pymongo
from pymongo import MongoClient
from quantdigger.datasource import import_data, ds_impl
from progressbar import ProgressBar

client = MongoClient()
db = client["quantdigger"]

class ImportData(Thread):
    def __init__(self, file, filepath, datasource, type):
        Thread.__init__(self)
        self._file = file
        self._type = type
        self._filepath = filepath
        self._datasource = datasource

    def run(self):
        with open(self._filepath) as f:
            lines = f.readlines()
            data = {
                'datetime': [],
                'open': [],
                'high': [],
                'low': [],
                'close': [],
                'volume': [],
                'turnover': []
            }

            for ln in lines[2:-1]:
                ln = ln.rstrip('\r\n').split('\t')
                if self._type > 95:
                    ln[0] = datetime.strptime(ln[0], "%Y/%m/%d") + \
                            timedelta(hours=15)
                    for i in range(1, len(ln)):
                        ln[i] = float(ln[i])
                    data['datetime'].append(ln[0])
                    data['open'].append(ln[1])
                    data['high'].append(ln[2])
                    data['low'].append(ln[3])
                    data['close'].append(ln[4])
                    data['volume'].append(ln[5])
                    data['turnover'].append(ln[6])
                else:
                    ln[0] = datetime.strptime(ln[0], "%Y/%m/%d") + \
                            timedelta(hours=int(ln[1][0:2]), minutes=int(ln[1][2:4]))
                    for i in range(2, len(ln)):
                        ln[i] = float(ln[i])
                    data['datetime'].append(ln[0])
                    data['open'].append(ln[2])
                    data['high'].append(ln[3])
                    data['low'].append(ln[4])
                    data['close'].append(ln[5])
                    data['volume'].append(ln[6])
                    data['turnover'].append(ln[7])

            t = self._file.split('#')
            exch = t[0]
            code = t[1].split('.')[0]
            ## 91 1m, 92 5m, 93 15m, 94 30m, 95 H, 97 D, 97 W, 98 M
            if self._type == 95:
                strpcon = "".join([code, '.', exch, '-', '1.Hour'])
            elif self._type == 94:
                strpcon = "".join([code, '.', exch, '-', '30.Minute'])
            elif self._type == 93:
                strpcon = "".join([code, '.', exch, '-', '15.Minute'])
            elif self._type == 92:
                strpcon = "".join([code, '.', exch, '-', '5.Minute'])
            elif self._type == 91:
                strpcon = "".join([code, '.', exch, '-', '1.Minute'])
            else: #96
                strpcon = "".join([code, '.', exch, '-', '1.Day'])

            # print strpcon, len(data['datetime'])
            if len(data['datetime']) > 0:
                self._datasource.import_bars(data, strpcon)
        print 'import data end:', self._filepath


def import_tdx_stock(path, ld, type):
    """ 导入通达信的股票数据

    Args:
        path (str): 数据文件夹
        ld (LocalData): 本地数据库对象
    """
    from datetime import datetime, timedelta
    # from quantdigger.util import ProgressBar

    for path, dirs, files in os.walk(path):
        # pbar = ProgressBar(maxval=len(files)).start()
        ImportDatas = []
        for file_ in files:
            filepath = path + os.sep + file_
            if filepath.endswith(".txt"):
                ImportDatas.append(ImportData(file_, filepath, ld, type))

        for c in ImportDatas:
            c.start()
        for c in ImportDatas:
            c.join()
                # with open(filepath) as f:
                #     lines = f.readlines()
                #     data = {
                #         'datetime': [],
                #         'open': [],
                #         'high': [],
                #         'low': [],
                #         'close': [],
                #         'volume': [],
                #         'turnover': []
                #     }
                #
                #     for ln in lines[2:-1]:
                #         ln = ln.rstrip('\r\n').split('\t')
                #         ln[0] = datetime.strptime(ln[0], "%Y/%m/%d") + \
                #             timedelta(hours=15)
                #         for i in range(1, len(ln)):
                #             ln[i] = float(ln[i])
                #         data['datetime'].append(ln[0])
                #         data['open'].append(ln[1])
                #         data['high'].append(ln[2])
                #         data['low'].append(ln[3])
                #         data['close'].append(ln[4])
                #         data['volume'].append(ln[5])
                #         data['turnover'].append(ln[6])
                #     t = file_.split('#')
                #     exch = t[0]
                #     code = t[1].split('.')[0]
                #     strpcon = "".join([code, '.', exch, '-', '1.Day'])
                #     # print strpcon, len(data['datetime'])
                #     if len(data['datetime']) > 0:
                #         ld.import_bars(data, strpcon)
            # pbar.next
            # pbar.log('')
        # pbar.finish()
    return

# def import_tdx_stock_min(path, ld, time):
#     """ 导入通达信的股票数据
#
#     Args:
#         path (str): 数据文件夹
#         ld (LocalData): 本地数据库对象
#     """
#     from datetime import datetime, timedelta
#     # from quantdigger.util import ProgressBar
#     for path, dirs, files in os.walk(path):
#         # progressbar = ProgressBar(total=len(files))
#         for file_ in files:
#             filepath = path + os.sep + file_
#             if filepath.endswith(".txt"):
#                 with open(filepath) as f:
#                     lines = f.readlines()
#                     data = {
#                         'datetime': [],
#                         'open': [],
#                         'high': [],
#                         'low': [],
#                         'close': [],
#                         'volume': [],
#                         'turnover': []
#                     }
#
#                     for ln in lines[2:-1]:
#                         ln = ln.rstrip('\r\n').split('\t')
#                         ln[0] = datetime.strptime(ln[0], "%Y/%m/%d") + \
#                                 timedelta(hours=int(ln[1][0:2]), minutes=int(ln[1][2:4]))
#                         for i in range(2, len(ln)):
#                             ln[i] = float(ln[i])
#                         data['datetime'].append(ln[0])
#                         data['open'].append(ln[2])
#                         data['high'].append(ln[3])
#                         data['low'].append(ln[4])
#                         data['close'].append(ln[5])
#                         data['volume'].append(ln[6])
#                         data['turnover'].append(ln[7])
#                     t = file_.split('#')
#                     exch = t[0]
#                     code = t[1].split('.')[0]
#                     strpcon = "".join([code, '.', exch, '-', time + '.Minute'])
#                     print strpcon, len(data['datetime'])
#                     # ld.import_bars(data, strpcon)
#             # progressbar.move()
#             # progressbar.log('')
#     return

mongo_ds = ds_impl.mongodb_source.MongoDBSource('localhost',27017,'min5')

# csv_ds = ds_impl.csv_source.CsvSource('data')

# import_tdx_stock('/opt/data/day3', mongo_ds, 96)
import_tdx_stock('/opt/data/fin5', mongo_ds, 92)

# import_tdx_stock('/opt/data/fin1', mongo_ds, 91)
# a="0945"
# print a.
# print timedelta(hours=int(a[0:2]), minutes=int(a[2:4]))