# import logging
from collections import Iterable
# from itertools import zip_longest
from functools import partial
import pandas as pd
from joblib import Parallel, delayed

from DB import DBPrice
from Indicator import Atr
from Util import Logger,SystemEnv
from Instrument import Symbol

#-----------------------------------------------------------------

def split_data(int_no_split,  list_symbol):
    """
    :param int_no_split:  int
    :param list_symbol: list
    :return: list
    """
    if list_symbol is None:
        raise TypeError('Symbol is not specified!')

    if not isinstance(list_symbol, Iterable):
        raise TypeError('Symbol must be an Iterable object!')

    if len(list_symbol) < int_no_split:
        return [[symbol] for symbol in list_symbol]

    symbol_cnt = int(len(list_symbol) / int_no_split)
    group_adjacent = lambda a, k: zip(*([iter(a)] * k))
    # 使用lambda函数group_adjacent将market_symbols切割子序列，每个子系列sub_symbols_cnt个
    group_symbol = list(group_adjacent(list_symbol, symbol_cnt))
    # 将不能整除的余数symbol个再放进去
    residue_ind = -(len(list_symbol) % symbol_cnt) if symbol_cnt > 0 else 0
    if residue_ind < 0:
        # 所以如果不能除尽，最终切割的子序列数量为k_split+1, 外部如果需要进行多认为并行，可根据最终切割好的数量重分配任务数
        group_symbol.append(list_symbol[residue_ind:])

    # group_symbol = zip_longest(*[iter(list_symbol)], fillvalue='empty') * symbol_cnt
    # type(group_symbol) =itertools.zip_longest

    return group_symbol


def calc_atr(kline_df=pd.DataFrame):
    """
    为输入的kline_df金融时间序列计算atr21和atr14，计算结果直接加到kline_df的atr21列和atr14列中
    :param kline_df: 金融时间序列pd.DataFrame对象
    """
    kline_df['atr21_ewm'] = 0
    kline_df['atr21_ma'] = 0
    if kline_df.shape[0] > 21:
        # 大于21d计算atr21
        kline_df['atr21_ewm'] = Atr.calc_atr_from_pd(kline_df['high'].values,
                                                     kline_df['low'].values,
                                                     kline_df['close'].values,
                                                     time_period=21)
        # 将前面的bfill
        kline_df['atr21_ewm'].fillna(method='bfill', inplace=True)

        # 大于21d计算atr21
        kline_df['atr21_ma'] = Atr.calc_atr_from_ta(kline_df['high'].values,
                                                    kline_df['low'].values,
                                                    kline_df['close'].values,
                                                    time_period=21)
        # 将前面的bfill
        kline_df['atr21_ma'].fillna(method='bfill', inplace=True)

    kline_df['atr14_ewm'] = 0
    kline_df['atr14_ma'] = 0
    if kline_df.shape[0] > 14:
        # 大于14d计算atr14
        kline_df['atr14_ewm'] = Atr.calc_atr_from_pd(kline_df['high'].values,
                                                     kline_df['low'].values,
                                                     kline_df['close'].values,
                                                     time_period=14)
        # 将前面的bfill
        kline_df['atr14_ewm'].fillna(method='bfill', inplace=True)

        # 大于14d计算atr14
        kline_df['atr14_ma'] = Atr.calc_atr_from_ta(kline_df['high'].values,
                                                    kline_df['low'].values,
                                                    kline_df['close'].values,
                                                    time_period=14)
        # 将前面的bfill
        kline_df['atr14_ma'].fillna(method='bfill', inplace=True)

    kline_df['returns'] = kline_df['adjclose'].pct_change()
    kline_df.fillna(0, inplace=True)

    return kline_df

def _kl_df_dict_parallel(choice_symbols, holding_period, start, end, benchmark):
    """
    多进程或者多线程被委托的任务函数，多任务批量获取时间序列数据
    :param choice_symbols: symbol序列
    :param holding_period: 请求几年的历史回测数据int
    :param start: 请求的开始日期 str对象
    :param end: 请求的结束日期 str对象
    :param benchmark: 资金回测时间标尺，AbuBenchmark实例对象
    :return: df_dict字典中key=请求symbol的str对象，value＝(save_kl_key: 提供外部进行保存, df: 金融时间序列pd.DataFrame对象)
    """
    df_dict = {}

    # 启动多进程进度条，如果是多线程暂时也启动了AbuMulPidProgress，需优化
    for epoch, symbol in enumerate(choice_symbols):
        # 迭代choice_symbols进行_make_kl_df, 注意_make_kl_df的参数save=False，即并行获取，不在内部save，要在外部save
            df  = DBPrice.select_price(symbol,holding_period=holding_period, start=start, end=end)
            df_dict[symbol] = df

    return df_dict

@Logger.elapse_time
def kl_df_dict_parallel(symbols, holding_period=2, start=None, end=None, benchmark=None,
                        n_jobs=-1, prefer='threads', verbose=3):
    """
    多进程或者多线程对外执行函数，多任务批量获取时间序列数据
    :param symbols: symbol序列
    :param holding_period: 请求几年的历史回测数据int
    :param start: 请求的开始日期 str对象
    :param end: 请求的结束日期 str对象
    :param benchmark: 资金回测时间标尺，AbuBenchmark实例对象
    :param n_jobs: 并行的任务数，对于进程代表进程数，线程代表线程数
                   -1 : all available CUPS
                   None :  1 CPU = backend -loky for debugging purpose
    :param prefer: process：多进程，thread：多线程，main：单进程单线程
    :param verbose: print more information on Parallel
    """

    # TODO Iterable和six.string_types的判断抽出来放在一个模块，做为Iterable的判断来使用
    if not isinstance(symbols, Iterable):
        raise TypeError('symbols must a Iterable obj!')

    # 可迭代的symbols序列分成n_jobs个子序列
    parallel_symbols = split_data(SystemEnv.g_cpu_cnt, symbols)

    # 因为切割会有余数，所以将原始设置的进程数切换为分割好的个数, 即32 -> 33 16 -> 17
    n_jobs = len(parallel_symbols)

    # 使用partial对并行函数_kl_df_dict_parallel进行委托
    parallel_func = partial(_kl_df_dict_parallel,
                            holding_period=holding_period,
                            start=start,
                            end=end,
                            benchmark=benchmark)

    parallel = Parallel(n_jobs=n_jobs, prefer=prefer, verbose=verbose)

    list_dict_df = parallel( delayed(parallel_func)(choice_stickers) for choice_stickers in parallel_symbols )

    df_dict = {}
    for dict_item in list_dict_df:
        for x, y in dict_item.items():
            df_dict[x]=y

    return  df_dict
    # if how == 'process':
    #     parallel = Parallel( n_jobs=n_jobs, prefer=how, verbose=0, pre_dispatch='2*n_jobs')
    #     df_dicts = parallel(delayed(parallel_func)(choice_stickers)  for choice_stickers in parallel_symbols)
    # elif how == 'thread':
    #     # 通过ThreadPoolExecutor进行线程并行任务
    #     with ThreadPoolExecutor(max_workers=n_jobs) as pool:
    #         k_use_map = True
    #         if k_use_map:
    #             df_dicts = list(pool.map(parallel_func, parallel_stickers))
    #         else:
    #             futures = [pool.submit(parallel_func, symbols) for symbols in parallel_stickers]
    #             df_dicts = [future.result() for future in futures if future.exception() is None]
    # elif how == 'main':
    #     # 单进程单线程
    #     df_dicts = [parallel_func(symbols) for symbols in parallel_symbols]
    # else:
    #     raise TypeError('ONLY process OR thread!')

    # if save:
    #     # 统一进行批量保存
    #     h5s_fn = ABuEnv.g_project_kl_df_data if ABuEnv.g_data_cache_type == EDataCacheType.E_DATA_CACHE_HDF5 else None
    #
    #     @batch_h5s(h5s_fn)
    #     def _batch_save():
    #         for df_dict in df_dicts:
    #             # 每一个df_dict是一个并行的序列返回的数据
    #             for ind, (key_tuple, df) in enumerate(df_dict.values()):
    #                 # (key_tuple, df)是保存kl需要的数据, 迭代后直接使用save_kline_df
    #                 save_kline_df(df, *key_tuple)
    #                 if df is not None:
    #                     print("save kl {}_{}_{} {}/{}".format(key_tuple[0].value, key_tuple[1], key_tuple[2], ind,
    #                                                           df.shape[0]))
    #             # 完成一层循环一次，即批量保存完一个并行的序列返回的数据后，进行清屏
    #             do_clear_output()
    #
    #     _batch_save()
    # return df_dicts


def make_kl_df(symbol, holding_period=2, start=None, end=None, benchmark=None,
               prefer='threads', show_progress=True, parallel=False):
    """
    :param symbol: list or Series or str
    :param holding_period: int
    :param start: start date
    :param end: end date
    :param benchmark:
    :param show_progress:
    :param parallel:
    :param prefer:
    :return:
    """

    # if isinstance(symbol, Instrument) or isinstance(symbol, str):
    if isinstance(symbol, str):
        # 对单个symbol进行数据获取
        # df, _ = _make_kl_df(symbol, data_mode=data_mode, n_folds=n_folds, start=start, end=end, benchmark=benchmark,
        #                     save=True)
        df = DBPrice.select_price(symbol,holding_period=holding_period, start=start, end=end)
        return df
    elif isinstance(symbol, (list, tuple, pd.Series, pd.Index)):
        panel = dict()
        df_dicts = kl_df_dict_parallel(symbol, holding_period=holding_period, start=start, end=end,
                                       benchmark=benchmark, prefer=prefer)
        return df_dicts
    else:
        raise TypeError('symbol type is error')

def get_price(symbol,holding_period=2, start_date=None, end_date=None):
    """
    通过make_kl_df获取金融时间序列后，只保留收盘价格，只是为了配合主流回测平台接口名称，适配使用
    :param symbol: str对象或Symbol对象
    :param holding_period: the numberg of years starting from the max(Pric Date)
    :param start_date: 请求的开始日期 str对象
    :param end_date: 请求的结束日期 str对象
    :return: 金融时间序列pd.DataFrame对象只有一个price列
    """
    df_prices = make_kl_df(symbol, holding_period=holding_period, start=start_date, end=end_date)
    if df_prices is not None:
        if isinstance(df_prices, dict):
            dict_df = {}
            for key, value in df_prices.items():
                # df = value.filter(['AdjClose'])
                # df.rename(columns={'AdjClose': 'price'})
                dict_df[key]=value

            return  dict_df
        else:
            # df = df_prices.filter(['close'])
            # 为了配合主流回测平台适配
            # return df.rename(columns={'close': 'price'})
            return  df_prices