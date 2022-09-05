"""
ATR

ATR又称 Average True Range平均真实波动范围，简称ATR指标，是由J.Welles Wilder 发明的，ATR指标主要是用来衡量市场波动的强烈度，
即为了显示市场变化率的指标。

计算方法：
1. TR=∣最高价-最低价∣，∣最高价-昨收∣，∣昨收-最低价∣中的最大值
2. 真实波幅（ATR）= MA(TR,N)（TR的N日简单移动平均）
3. 常用参数N设置为14日或者21日
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from enum import Enum
from .IndicatorBase import ECalcType, g_calc_type
from Util import ScalerUtil


"""
    彻底不用talib，完全都使用自己计算的指标结果
    g_calc_type = ECalcType.E_FROM_PD
"""


def calc_atr_from_ta(high, low, close, time_period=14):
    """
    使用talib计算atr，即透传talib.ATR计算结果
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :param time_period: atr的N值默认值14，int
    :return: atr值序列，np.array对象
    """
    import talib
    if isinstance(high, pd.Series):
        high = high.values
    if isinstance(low, pd.Series):
        low = low.values
    if isinstance(close, pd.Series):
        close = close.values
    # （ATR）= MA(TR, N)（TR的N日简单移动平均）, 这里没有完全按照标准公式使用简单移动平均，使用了pandas.series.ewm()mean()，即加权移动平均
    atr = talib.ATR(high, low, close, timeperiod=time_period)
    return atr


def calc_atr_from_pd(high, low, close, time_period=14):
    """
    通过atr公式手动计算atr
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :param time_period: atr的N值默认值14，int
    :return: atr值序列，np.array对象
    """
    if isinstance(close, pd.Series):
        # shift(1)构成昨天收盘价格序列
        pre_close = close.shift(1).values
    else:
        from scipy.ndimage.interpolation import shift
        # 也可以暂时转换为pd.Series进行shift
        pre_close = shift(close, 1)
    pre_close[0] = pre_close[1]

    if isinstance(high, pd.Series):
        high = high.values
    if isinstance(low, pd.Series):
        low = low.values

    # ∣最高价 - 最低价∣
    tr_hl = np.abs(high - low)
    # ∣最高价 - 昨收∣
    tr_hc = np.abs(high - pre_close)
    # ∣昨收 - 最低价∣
    tr_cl = np.abs(pre_close - low)
    # TR =∣最高价 - 最低价∣，∣最高价 - 昨收∣，∣昨收 - 最低价∣中的最大值
    tr = np.maximum(np.maximum(tr_hl, tr_hc), tr_cl)
    # （ATR）= MA(TR, N)（TR的N日简单移动平均）, 这里没有完全按照标准公式使用简单移动平均，使用了pandas.series.ewm()mean()，即加权移动平均
    atr = pd.Series(tr).ewm(span=time_period, min_periods=1).mean()

    # 返回atr值序列，np.array对象
    return atr.values


calc_atr = calc_atr_from_pd if g_calc_type == ECalcType.E_FROM_PD else calc_atr_from_ta


def atr14(high, low, close):
    """
    通过high, low, close计算atr14序列值
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :return: atr值序列，np.array对象
    """
    atr = calc_atr(high, low, close, 14)
    return atr


def atr21(high, low, close):
    """
    通过high, low, close计算atr21序列值
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :return: atr值序列，np.array对象
    """
    atr = calc_atr(high, low, close, 21)
    return atr


def atr14_min(high, low, close):
    """
    确定常数阀值时使用，通过high, low, close计算atr14序列值，返回计算结果atr14序列中的最小值
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :return: atr值序列，atr14序列中的最小值，float
    """
    _atr14 = atr14(high, low, close)
    _atr14 = pd.Series(_atr14)
    _atr14.fillna(method='bfill', inplace=True)
    _atr14 = _atr14.min()
    return _atr14


def atr21_min(high, low, close):
    """
    确定常数阀值时使用，通过high, low, close计算atr21序列值，返回计算结果atr21序列中的最小值
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :return: atr值序列，atr21序列中的最小值，float
    """
    _atr21 = atr21(high, low, close)
    _atr21 = pd.Series(_atr21)
    _atr21.fillna(method='bfill', inplace=True)
    _atr21 = _atr21.min()
    return _atr21


def atr21_max(high, low, close):
    """
    确定常数阀值时使用，通过high, low, close计算atr21序列值，返回计算结果atr21序列中的最大值
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :return: atr值序列，atr21序列中的最大值，float
    """
    _atr21 = atr21(high, low, close)
    _atr21 = pd.Series(_atr21)
    _atr21.fillna(method='bfill', inplace=True)
    _atr21 = _atr21.max()
    return _atr21


def plot_atr(high, low, close, kl_index, with_points=None, with_points_ext=None, time_period=14):
    """
    分别在上下两个子画布上绘制收盘价格，以及对应的atr曲线，如果有with_points点位标注，
    则只画在一个画布上，且将两个曲线进行缩放到一个数值级别
    :param high: 最高价格序列，pd.Series或者np.array
    :param low: 最低价格序列，pd.Series或者np.array
    :param close: 收盘价格序列，pd.Series或者np.array
    :param kl_index: pd.Index时间序列
    :param with_points: 这里的常规用途是传入买入order, with_points=buy_index=pd.to_datetime(orders['buy_date']))
    :param with_points_ext: 这里的常规用途是传入卖出order, with_points_ext=sell_index=pd.to_datetime(orders['sell_date']))
    :param time_period: atr的N值默认值14，int
    """
    atr = calc_atr(high, low, close, time_period)

    plt.figure(figsize=(14, 7))
    if with_points is not None or with_points_ext is not None:
        # 如果需要标准买入卖出点，就绘制在一个画布上
        p1 = plt.subplot(111)
        p2 = p1
        # 绘制在一个画布上, 将两个曲线进行缩放到一个数值级别
        matrix = ScalerUtil.scaler_matrix([atr, close])
        atr, close = matrix[matrix.columns[0]], matrix[matrix.columns[1]]

        # with_points和with_points_ext的点位使用竖线标注
        if with_points is not None:
            p1.axvline(with_points, color='green', linestyle='--')

        if with_points_ext is not None:
            p1.axvline(with_points_ext, color='red')
    else:
        # 绘制在两个子画布上面
        p1 = plt.subplot(211)
        p2 = plt.subplot(212)

    p1.plot(kl_index, close, "b-", label="close")
    p2.plot(kl_index, atr, "r-.", label="period={} atr".format(time_period), lw=2)
    p1.grid(True)
    p1.legend()
    p2.grid(True)
    p2.legend()

    plt.show()

#
# https://stackoverflow.com/questions/40256338/calculating-average-true-range-atr-on-ohlc-data-with-python
# def wwma(values, n):
#     """
#      J. Welles Wilder's EMA
#     """
#     return values.ewm(alpha=1/n, adjust=False).mean()
#
# def atr(df, n=14):
#     data = df.copy()
#     high = data[HIGH]
#     low = data[LOW]
#     close = data[CLOSE]
#     data['tr0'] = abs(high - low)
#     data['tr1'] = abs(high - close.shift())
#     data['tr2'] = abs(low - close.shift())
#     tr = data[['tr0', 'tr1', 'tr2']].max(axis=1)
#     atr = wwma(tr, n)
#     return atr


