#!/usr/bin/python
# encoding: utf-8

"""
@author:
contact:
@file:mdemo.py
@time: 17-3-9 下午7:59
"""

#from quantdigger.engine.series import NumberSeries
#from quantdigger.indicators.common import MA
#from quantdigger.util import  pcontract
from quantdigger import *

class DemoStrategy(Strategy):
    """ 策略A1 """

    def on_init(self, ctx):
        """初始化数据"""
        ctx.ma10 = MA(ctx.close, 10, 'ma10', 'y', 2)
        ctx.ma20 = MA(ctx.close, 20, 'ma20', 'b', 2)

    def on_symbol(self, ctx):
        """  选股 """
        return

    def on_bar(self, ctx):
        if ctx.curbar > 20:
            if ctx.ma10[2] < ctx.ma20[2] and ctx.ma10[1] > ctx.ma20[1]:
                ctx.buy(ctx.close, 1)
            elif ctx.pos() > 0 and ctx.ma10[2] > ctx.ma20[2] and \
                 ctx.ma10[1] < ctx.ma20[1]:
                ctx.sell(ctx.close, ctx.pos())

    def on_exit(self, ctx):
        return

class DemoStrategy2(Strategy):
    """ 策略A2 """

    def on_init(self, ctx):
        """初始化数据"""
        ctx.ma5 = MA(ctx.close, 5, 'ma5', 'y', 2)
        ctx.ma10 = MA(ctx.close, 10, 'ma10', 'black', 2)

    def on_symbol(self, ctx):
        """  选股 """
        return

    def on_bar(self, ctx):
        if ctx.curbar > 10:
            if ctx.ma5[2] < ctx.ma10[2] and ctx.ma5[1] > ctx.ma10[1]:
                ctx.buy(ctx.close, 1)
            elif ctx.pos() > 0 and ctx.ma5[2] > ctx.ma10[2] and \
                 ctx.ma5[1] < ctx.ma10[1]:
                ctx.sell(ctx.close, ctx.pos())

    def on_exit(self, ctx):
        return

if __name__ == '__main__':
    # set_symbols(['600718.SH-30.Minute'], 0)
    set_symbols(['002355.SZ-1.Hour'], 0)
    # set_symbols(['300310.SZ-1.Day'], 0)
    # 创建组合策略
    # 初始资金5000， 两个策略的资金配比为0.2:0.8
    profile = add_strategy([DemoStrategy('A1'), DemoStrategy2('A2')], { 'captial': 5000,
                              'ratio': [0.2, 0.8] })
    run()

    # 绘制k线，交易信号线
    from quantdigger.digger import finance, plotting

    # Orig
    # plotting.plot_strategy(profile.data(0), profile.indicators(1), profile.deals(1))
    # plotting.plot_strategy(profile.data(0), {}, profile.deals(1))

    # 绘制策略A1, 策略A2, 组合的资金曲线
    curve0 = finance.create_equity_curve(profile.all_holdings(0))
    curve1 = finance.create_equity_curve(profile.all_holdings(1))
    curve = finance.create_equity_curve(profile.all_holdings())
    # plotting.plot_curves([curve0.equity, curve1.equity, curve.equity],
    #                     colors=['r', 'g', 'b'],
    #                     names=[profile.name(0), profile.name(1), 'A0'])
    # 绘制净值曲线
    plotting.plot_curves([curve.networth])
    # 打印统计信息
    print finance.summary_stats(curve, 252*4*60)
