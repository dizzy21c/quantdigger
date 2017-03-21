# -*- coding: utf-8 -*-

# @file code_search3.py
# @brief 选股的例子
# @author wondereamer
# @version 0.2
# @date 2015-12-09

from quantdigger import *
from datetime import datetime
import time

class DemoStrategy(Strategy):
    """ 策略A1 """

    def __init__(self, name, cur_date):
        super(DemoStrategy, self).__init__(name)
        self.candicates = []
        self.to_sell = []
        self.cur_date = cur_date

    def on_init(self, ctx):
        """初始化数据"""
        # print 'on_init', ctx
        ctx.ma10 = MA(ctx.close, 5, 'ma10', 'y', 2)
        ctx.ma20 = MA(ctx.close, 20, 'ma20', 'b', 2)

    def on_symbol(self, ctx):
        # print ctx
        if ctx.curbar > 20 and ctx.datetime >= self.cur_date:
            # print ctx.ma10
            if ctx.ma10[1] < ctx.ma20[1] and ctx.ma10 > ctx.ma20:
                self.candicates.append(ctx.symbol)
                print ctx.symbol, ctx.datetime, ctx.close
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
        print("策略运行结束．")
        return


if __name__ == '__main__':
    t1 = time.time()
    print "begin=", datetime.now()
    #
    # set_symbols(['*.SZ'],dt_start="2017-01-01")
    # set_symbols(['600718.SH-1.Day'], dt_start="2017-01-01")
    set_symbols(['600718.SH-1.Day'], dt_start="2016-10-01")
    # set_symbols(['000676.SZ-1.Day'], dt_start="2016-01-01")
    algo = DemoStrategy('A1', datetime(2017,2,20))
    profile = add_strategy([algo], {'capital': 500000000.0})

    run()

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