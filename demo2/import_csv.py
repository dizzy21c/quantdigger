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
import multiprocessing

import pymongo
# from pymongo import MongoClient
from quantdigger.datasource import import_data, ds_impl

# client = MongoClient()
# db = client["quantdigger"]

def mpimport(file, filepath, datasource, type):
    with open(filepath) as f:
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
        t = file.split('#')
        exch = t[0]
        code = t[1].split('.')[0]
        strpcon = "".join([code, '.', exch, '-', '1.Day'])
        datasource.import_bars(data, strpcon)



def ImportData_min(file, filepath, datasource, type, time):
    with open(filepath) as f:
        with open(filepath) as f:
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
            t = file.split('#')
            exch = t[0]
            code = t[1].split('.')[0]
            strpcon = "".join([code, '.', exch, '-', time + '.Minute'])
            datasource.import_bars(data, strpcon)

def ImportData_min2(file, filepath, datasource, type, time, time2):
    ntimes = time2 / time
    with open(filepath) as f:
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
        newrow = False
        preday = None

        flg = 0
        cdata_open = 0.0
        cdata_high = 0.0
        cdata_low = 0.0
        cdata_close = 0.0
        cdata_volume = 0
        cdata_turnover = 0.0
        isoutput = False

        for ln in lines[2:-1]:
            ln = ln.rstrip('\r\n').split('\t')

            day = datetime.strptime(ln[0], "%Y/%m/%d")
            hour = timedelta(hours=int(ln[1][0:2]), minutes=int(ln[1][2:4]))

            ln[0] = datetime.strptime(ln[0], "%Y/%m/%d") + \
                    timedelta(hours=int(ln[1][0:2]), minutes=int(ln[1][2:4]))

            for i in range(2, len(ln)):
                ln[i] = float(ln[i])

            if preday == None or flg == ntimes:
                preday = day
                newrow = True
                cdata_open = ln[2]
                cdata_high = 0.0
                cdata_low = 0.0
                cdata_close = 0.0
                cdata_volume = 0
                cdata_turnover = 0.0
                flg = 0

            cdata_high = max(ln[3], cdata_high)
            cdata_low = min(ln[4], cdata_low)
            cdata_close = ln[5]
            cdata_volume += ln[6]
            cdata_turnover += ln[7]

            flg += 1
            if flg % ntimes == 0:
                data['datetime'].append(ln[0])
                data['open'].append(cdata_open)
                data['high'].append(cdata_high)
                data['low'].append(cdata_low)
                data['close'].append(cdata_close)
                data['volume'].append(cdata_volume)
                data['turnover'].append(cdata_turnover)
        t = file.split('#')
        exch = t[0]
        code = t[1].split('.')[0]
        if time2 == 60:
            strpcon = "".join([code, '.', exch, '-', '1.Hour'])
        else:
            strpcon = "".join([code, '.', exch, '-', str(time2) + '.Minute'])

        datasource.import_bars(data, strpcon)

def start_process():
    # print 'Starting', multiprocessing.current_process().name
    pass

def import_tdx_stock(path, ld, time=None, time2=0):
    """ 导入通达信的股票数据
    Args:
        path (str): 数据文件夹
        ld (LocalData): 本地数据库对象
    """
    from datetime import datetime, timedelta
    # from quantdigger.util import ProgressBar
    pool_size = multiprocessing.cpu_count() * 2
    # pool = multiprocessing.Pool(processes=pool_size, initializer = start_process,)
    pool = multiprocessing.Pool(processes=pool_size, initializer = start_process,)

    for path, dirs, files in os.walk(path):
        for file_ in files:
            filepath = path + os.sep + file_
            if filepath.endswith(".txt"):
                if time == None:
                    pool.apply_async(mpimport, args=(file_, filepath, ld, type,))
                else:
                    if time2 > 0:
                        pool.apply_async(ImportData_min2, args=(file_, filepath, ld, type,time, time2))
                    else:
                        pool.apply_async(ImportData_min, args=(file_, filepath, ld, type, time, ))

        # print('Waiting for all subprocesses done...')
        pool.close()
        pool.join()
        # print('All subprocesses done.')

    return


# mongo_ds = ds_impl.mongodb_source.MongoDBSource('localhost','27017','quantdigger')

print "begin=" , datetime.now()
csv_ds = ds_impl.csv_source.CsvSource('data')
import_tdx_stock('/opt/data/day3', csv_ds)
print "5min=" , datetime.now()
# import_tdx_stock('/opt/data/fin5', csv_ds, time="5")
print "1min=" , datetime.now()
# import_tdx_stock('/opt/data/fin5', csv_ds, time="1")
print "1min=" , datetime.now()
# import_tdx_stock('/opt/data/fin5', csv_ds, 5, 15)
print "1min=" , datetime.now()
# import_tdx_stock('/opt/data/fin5', csv_ds, 5, 30)
print "1min=" , datetime.now()
# import_tdx_stock('/opt/data/fin5', csv_ds, 5, 60)
# a="0945"
# print a.
# print timedelta(hours=int(a[0:2]), minutes=int(a[2:4]))
print "end=" , datetime.now()