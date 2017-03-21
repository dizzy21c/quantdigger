# -*- coding: utf-8 -*-

# @file code_search3.py
# @brief 选股的例子
# @author wondereamer
# @version 0.2
# @date 2015-12-09

from quantdigger import *
from quantdigger.datasource import ds_impl
from datetime import datetime
import time
from threading import Thread, current_thread, Lock, Condition
import os

class DemoStrategy(Strategy):
    """ 策略A1 """

    def __init__(self, name, cur_date, lock, file):
        super(DemoStrategy, self).__init__(name)
        self.candicates = []
        self.to_sell = []
        self._lock = lock
        self._file = file
        self.cur_date = cur_date

    def on_init(self, ctx):
        """初始化数据"""
        ctx.ma10 = MA(ctx.close, 7, 'ma10', 'y', 2)
        ctx.ma20 = MA(ctx.close, 18, 'ma20', 'b', 2)

    def on_symbol(self, ctx):
        if ctx.curbar > 20 and ctx.datetime >= self.cur_date:
            if ctx.ma10[1] < ctx.ma20[1] and ctx.ma10 > ctx.ma20:
                self.candicates.append((ctx.symbol, ctx.close[0], ctx.datetime[0]))
                # self._lock.acquire()
                # self._file.write(ctx.symbol  +  ', ' +  ctx.datetime + '\n')
                # self._lock.release()
                # print ctx.symbol, ctx.datetime, ctx.close
            # elif ctx.ma10[1] < ctx.ma20[1]:
            #     self.to_sell.append(ctx.symbol)

    def on_bar(self, ctx):
        # for symbol in self.to_sell:
        #     if ctx.pos('long', symbol) > 0:
        #         ctx.sell(ctx[symbol].close, 1, symbol)
        #         # print "sell:", symbol
        #
        # for symbol in self.candicates:
        #     if ctx.pos('long', symbol) == 0:
        #         ctx.buy(ctx[symbol].close, 1, symbol)
        #         # print "buy:", symbol
        #
        # self.candicates = []
        # self.to_sell = []
        return

    def on_exit(self, ctx):
        if len(self.candicates) <= 0:
            return

        self._lock.acquire()
        for data in self.candicates:
            # if len(self.candicates) > 0:
            self._file.write(data[0] + ',' + str(data[1]) + "," + data[2].strftime('%Y-%m-%d %H:%M:%S' + '\n'))
            # print "out=", data[0], str(data[1]), data[2].strftime('%Y-%m-%d %H:%M:%S')
        self._lock.release()
        # print("策略运行结束．")
        return


class Find(Thread):
    def __init__(self, contract, dt_start, dt_end, strategy, lock, file):
        Thread.__init__(self)
        self._contract = contract
        self._begdate = dt_start
        self._enddate = dt_end
        self._strategy = strategy

    def run(self):
        set_symbols(self._contract, dt_start=self._begdate)
        algo = DemoStrategy(self._strategy, self._enddate, lock, file)
        profile = add_strategy([algo], {'capital': 500000.0})
        run()
        # try:
        #     run()
        # except:
        #     print 'calc error:' + ' '.join(self._contract)

if __name__ == '__main__':
    t1 = time.time()
    print "begin=", datetime.now()
    csv_ds = ds_impl.csv_source.CsvSource('data')
    contracts = csv_ds.get_contracts().index
    threads = []
    lock = Condition()
    idx=0
    lcontracts = []
    dt_begin="2016-01-01"
    dt_end=datetime(2017,3,7)
    file=open('calc/out-' + dt_end.strftime('%Y-%m-%d'), 'a+')
    for contract in contracts:
        if contract[0:1] == 'A':
            continue
        idx += 1
        lcontracts.append(contract + '-' + "1.Day")
        if idx % 50 == 0:
            threads.append(Find(lcontracts, dt_begin, dt_end, 'A1', lock, file))
            lcontracts = []
        # if idx > 21:
        #     break

    if len(lcontracts) > 0:
        threads.append(Find(lcontracts, dt_begin, dt_end, 'A1', lock, file))

    for t in threads:
        t.start();

    for t in threads:
        t.join()

    file.close()

    #
    # set_symbols(['*.SZ'],dt_start="2017-01-01")
    # set_symbols(['600718.SH-1.Day'], dt_start="2017-01-01")
    # set_symbols(['600000.SH-1.Day'], dt_start="2017-01-01")
    # set_symbols(['000676.SZ-1.Day'], dt_start="2016-01-01")
    # algo = DemoStrategy('A1', datetime(2017,3,3))
    # profile = add_strategy([algo], {'capital': 500000000.0})

    # run()

    # set_symbols(['*.SH'],dt_start="2017-01-01")
    # # set_symbols(['600718.SH-1.Day'], dt_start="2017-01-01")
    # # set_symbols(['600000.SH-1.Day'], dt_start="2017-01-01")
    # # set_symbols(['000676.SZ-1.Day'], dt_start="2016-01-01")
    # algo = DemoStrategy('A1', datetime(2017,02,24))
    # profile = add_strategy([algo], {'capital': 500000000.0})
    #
    # run()
    # from quantdigger.digger import finance, plotting
    # curve = finance.create_equity_curve(profile.all_holdings())
    #plotting.plot_strategy(profile.data('AA.SHFE-1.Minute'), profile.technicals(0),
                            #profile.deals(0), curve.equity.values)
    ## 绘制净值曲线
    # plotting.plot_curves([curve.networth])
    print (time.time() - t1)
    print "end=", datetime.now()