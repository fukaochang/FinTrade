# all methods can be imported at once
# from yahoo_fin.stock_info import *
# or
import pandas as pd
import yahoo_fin.stock_info as si
from functools import reduce


def get_data(tickers, start_date=str, end_date=str, index_as_date=True, interval='1d') -> pd.DataFrame:
    """
    :param start_date:
    :param end_date:
    :param index_as_date:
    :param interval:
    :param tickers is a list of Equity tickers:
    :return :  
    """
    # DatetimeIndex: 10182  entries, 1980 - 12 - 12   to    2021 - 04 - 30
    price_data = {ticker: si.get_data(ticker.strip(), start_date, end_date, index_as_date, interval)
                  for ticker in tickers}

    combined = reduce(lambda x, y: x.append(y), price_data.values())
    # combined = reduce(lambda x, y: x.concat(y), price_data.values())
    return combined


def get_balance_sheet(tickers, yearly=True):
    dict_data = {ticker: si.get_balance_sheet(ticker,yearly) for ticker in tickers}
    return dict_data

def get_income_statement(tickers, yearly=True):
    dict_data = {ticker: si.get_income_statement(ticker, yearly) for ticker in tickers}
    return dict_data

def get_cash_flow(tickers, yearly=True):
    dict_data = {ticker: si.get_cash_flow(ticker, yearly) for ticker in tickers}
    return dict_data


# Analysts = si.get_analysts_info(ticker)
# print (Analysts)

# balance_sheet = si.get_balance_sheet(ticker,yearly = False)
# print (balance_sheet)
