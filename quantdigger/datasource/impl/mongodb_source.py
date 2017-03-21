# -*- coding: utf-8 -*-

import pandas as pd
import pymongo
import json
from pymongo import MongoClient

from quantdigger.datasource import datautil
from quantdigger.datasource.dsutil import *
from quantdigger.datasource.source import SourceWrapper, DatasourceAbstract


@register_datasource('mongodb', 'address', 'port', 'dbname')
class MongoDBSource(DatasourceAbstract):
    '''MongoDBs数据源'''

    def __init__(self, address='localhost', port=27017, dbname='stock'):
        # TODO address, port
        self._client = MongoClient(address, port)
        self._db = self._client[dbname]

    def _get_collection_name(self, period, exchange, code):
        return '{period}.{exchange}.{code}'.format(
            period=str(period).replace('.', ''),
            exchange=exchange,
            code=code)

    def _parse_collection_name(self, collection_name):
        return collection_name.split('.')

    def get_bars(self, pcontract, dt_start, dt_end):
        dt_start = pd.to_datetime(dt_start)
        dt_end = pd.to_datetime(dt_end)
        id_start, _ = datautil.encode2id(pcontract.period, dt_start)
        id_end, _ = datautil.encode2id(pcontract.period, dt_end)
        colname = self._get_collection_name(
            pcontract.period,
            pcontract.contract.exchange,
            pcontract.contract.code)
        cursor = self._db[colname].find({
            '_id': {
                '$gt': id_start,
                '$lt': id_end
            }
        }).sort('_id', pymongo.ASCENDING)
        data = pd.DataFrame(list(cursor)).set_index('datetime')
        return SourceWrapper(pcontract, data, len(data))

    def get_last_bars(self, pcontract, n):
        raise NotImplementedError

    def get_contracts(self):
        colname = 'contract'
        cursor = self._db[colname].find()
        return pd.DataFrame(list(cursor))

    def get_code2strpcon(self):
        symbols = {}
        period_exchange2strpcon = {}
        names = self._db.collection_names()
        symbols = {}
        period_exchange2strpcon = {}
        for name in filter(lambda n: n == 'system.indexes', names):
            period, exch, code = self._parse_collection_names(name)
            period_exch = '%s-%s' % (exch, period)
            strpcon = '%s.%s' % (code, period_exch)
            lst = symbols.setdefault(code, [])
            lst.append(strpcon)
            lst = period_exchange2strpcon(period_exch, [])
            lst.append(strpcon)
            return symbols, period_exchange2strpcon

    def import_bars(self, tbdata, pcontract):
        """ 导入交易数据

        Args:
            tbdata (dict): {'datetime', 'open', 'close',
                            'high', 'low', 'volume'}
            pcontract (PContract): 周期合约
            pcontract = '600261.SH-1.Day'
            =>
            {周期}.{交易所}.{合约代码} = 1DAY.SHFE.BB
        """
        strpcon = str(pcontract).upper()
        contract, period = tuple(strpcon.split('-'))
        code, exch = tuple(contract.split('.'))
        period2 = period.replace('.', '')
        table = "%s.%s.%s" % (period2, exch, code)

        df = pd.DataFrame(tbdata)
        # df['_id'] = datautil.encode2id(df['datetime'])[1]
        df['_id'] = df['datetime'].map(lambda x: datautil.encode2id(period, x)[1])
        df['datetime'] = pd.to_datetime(df['datetime'])
        # df[]
        # df.
        records = json.loads(df.T.to_json(date_format='iso')).values()
        self._db[table].insert_many(records)
        # df.to_csv(fname, columns=[
        #     'datetime', 'open', 'close', 'high', 'low', 'volume'
        # ], index=False)


    def import_contracts(self, data):
        """ 导入合约的基本信息。

        Args:
            data (dict): {key, code, exchange, name, spell,
            long_margin_ratio, short_margin_ratio, price_tick, volume_multiple}

        """
        # fname = os.path.join(self._root, "CONTRACTS.csv")
        df = pd.DataFrame(data)
        self._db
        # df.to_csv(fname, columns=[
        #     'code', 'exchange', 'name', 'spell',
        #     'long_margin_ratio', 'short_margin_ratio', 'price_tick',
        #     'volume_multiple'
        # ], index=False)

