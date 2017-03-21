# -*- coding: utf-8 -*-
import pandas as pd
from quantdigger import ConfigUtil
from quantdigger.datasource import ds_impl
from xpinyin import Pinyin
import types

def import_contracts(decode=False):
    """ 从文件导入合约到数据库 """
    df = pd.read_csv('/home/zjx/PycharmProjects/pythonlean/other/contracts.txt')
    contracts = []
    codes = set()
    for idx, row in df.iterrows():
        data = None
        if row['isStock']:
            if row['exchangeId'] == 'SZSE':
                row['exchangeId'] = 'SZ'
            else:
                row['exchangeId'] = 'SH'
            if decode:
                data = (row['code']+'.'+row['exchangeId'],
                        row['code'],
                        row['exchangeId'],
                        row['name'].decode('utf8'), row['pinyin'], 1, 1, 0, 1)
            else:
                data = (row['code']+'.'+row['exchangeId'],
                        row['code'],
                        row['exchangeId'],
                        row['name'], row['pinyin'], 1, 1, 0, 1)
            contracts.append(data)
        else:
            data = (row['code']+'.'+row['exchangeId'],
                    row['code'],
                    row['exchangeId'],
                    row['code'], row['code'], row['long_margin_ratio'],
                    row['short_margin_ratio'],
                    row['price_tick'],
                    row['volume_multiple'])
            contracts.append(data)
            # 修正ctp部分合约只有3位日期。
            if not row['code'][-4].isdigit():
                row['code'] = row['code'][0:-3] + '1' + row['code'][-3:]
                # 支持两种日期
                data = (row['code']+'.'+row['exchangeId'],
                        row['code'],
                        row['exchangeId'],
                        row['code'], row['code'], row['long_margin_ratio'],
                        row['short_margin_ratio'],
                        row['price_tick'],
                        row['volume_multiple'])
                contracts.append(data)
            # 无日期指定的期货合约
            code = row['code'][0:-4]
            if code not in codes:
                t = (code+'.'+row['exchangeId'], code, row['exchangeId'],
                     code, code, row['long_margin_ratio'],
                     row['short_margin_ratio'], row['price_tick'],
                     row['volume_multiple'])
                contracts.append(t)
                codes.add(code)
    contracts = zip(*contracts)
    rst = {
            'key': contracts[0],
            'code': contracts[1],
            'exchange': contracts[2],
            'name': contracts[3],
            'spell': contracts[4],
            'long_margin_ratio': contracts[5],
            'short_margin_ratio': contracts[6],
            'price_tick': contracts[7],
            'volume_multiple': contracts[8],
            }
    return rst

def import_contracts_tdx(decode=False):
    """ 从文件导入合约到数据库 """
    # df = pd.read_csv('/opt/data/hsag-out.csv')
    df = pd.read_table('/opt/data/hsag-out.txt')
    contracts = []
    codes = set()
    for idx, row in df.iterrows():
        data = None
        exchangeId = None
        # 代码	名称	涨幅%%	现价	涨跌	买价	卖价	总量	现量	涨速%%	换手%%	今开	最高	最低	昨收	市盈(动)	总金额	量比	细分行业	地区	振幅%%	均价	内盘	外盘
        #   1   2   3       4   5   6   7   8   9   10      11      12  13  14  15  16      17      18  19      20  21      22  23  24
        # 000001	平安银行	-0.63	9.51	-0.06	9.50	9.51	335328	3047	0.00	          0.20	9.55	9.57	9.48	9.57	     6.54	319293824	  0.59	银行	深圳	0.94	9.52	194779	140548
        #   1       2       3       4       5       6       7       8       9       10              11      12      13      14      15          16      17              18  19  20  21      22      23      24
        if row['代码']:
            code=row['代码']
            if type(row['名称']) is types.StringType :
                name = row['名称'].decode('utf8')
            else:
                code = 'A00001'
                exchangeId = 'OT'
                data = (code+'.'+exchangeId,
                        code,
                        exchangeId,
                        code, 'FOR-SPE', 1, 1, 0.01, 100,'','')
                contracts.append(data)
                break

            # print name,p.get_initials(name, u'')
            if code[0:1] == '6':
                exchangeId = 'SH'
            # elif code[0:1] == '3':
            #     exchangeId = 'CY'
            # elif code[0:3] == '002':
            #     exchangeId = 'ZX'
            else:
                exchangeId = 'SZ'
            if not decode:
                # 修改内容
                # self.assertEqual(self.p.get_pinyin(u'上海'), u'shang-hai')
                # self.assertEqual(self.p.get_initials(u'你好', u''), u'NH')
                volume_multiple=100
                # print row, type(row['地区'])
                if type(row['地区']) is types.StringType:
                    region = row['地区'].decode('utf8')
                    trade = row['细分行业'].decode('utf8')
                else:
                    region='None'
                    trade = 'None'

                data = (code+'.'+exchangeId,
                        code,
                        exchangeId,
                        p.get_pinyin(name), p.get_initials(name, u''), 1, 1, 0.01, 100, region, trade)
            else:
                data = (row['code']+'.'+row['exchangeId'],
                        row['code'],
                        row['exchangeId'],
                        row['name'], row['pinyin'], 1, 1, 0, 1)
            contracts.append(data)
        else:
            data = (row['code']+'.'+row['exchangeId'],
                    row['code'],
                    row['exchangeId'],
                    row['code'], row['code'], row['long_margin_ratio'],
                    row['short_margin_ratio'],
                    row['price_tick'],
                    row['volume_multiple'])
            contracts.append(data)
            # 修正ctp部分合约只有3位日期。
            if not row['code'][-4].isdigit():
                row['code'] = row['code'][0:-3] + '1' + row['code'][-3:]
                # 支持两种日期
                data = (row['code']+'.'+row['exchangeId'],
                        row['code'],
                        row['exchangeId'],
                        row['code'], row['code'], row['long_margin_ratio'],
                        row['short_margin_ratio'],
                        row['price_tick'],
                        row['volume_multiple'])
                contracts.append(data)
            # 无日期指定的期货合约
            code = row['code'][0:-4]
            if code not in codes:
                t = (code+'.'+row['exchangeId'], code, row['exchangeId'],
                     code, code, row['long_margin_ratio'],
                     row['short_margin_ratio'], row['price_tick'],
                     row['volume_multiple'])
                contracts.append(t)
                codes.add(code)
    contracts = zip(*contracts)
    rst = {
            'key': contracts[0],
            'code': contracts[1],
            'exchange': contracts[2],
            'name': contracts[3],
            'spell': contracts[4],
            'long_margin_ratio': contracts[5],
            'short_margin_ratio': contracts[6],
            'price_tick': contracts[7],
            'volume_multiple': contracts[8],
            'region': contracts[9],
            'trade': contracts[10],
            }
    return rst

print("import contracts..")
p = Pinyin()
contracts = import_contracts_tdx()
# contracts = import_contracts()
csv_ds = ds_impl.csv_source.CsvSource('data')
csv_ds.import_contracts(contracts)

# contracts = import_contracts(True)
#sqlite_ds = ds_impl.sqlite_source.SqliteSource('../data/digger.db')
# sqlite_ds.import_contracts(contracts)
