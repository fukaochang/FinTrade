# all methods can be imported at once
# from yahoo_fin.stock_info import *
# or
"""

http://theautomatic.net/yahoo_fin-documentation/

What is yahoo_fin?
  Yahoo_fin is a Python 3 package designed to scrape historical stock price data, as well as to provide current
information on market caps, dividend yields, and which stocks comprise the major exchanges.
Additional functionality includes scraping income statements, balance sheets, cash flows, holder information,
and analyst data.
  The package includes the ability to scrape live (real-time) stock prices, capture cryptocurrency data,
and get the most actively traded stocks on a current trading day.

  Yahoo_fin also contains a module for retrieving option prices and expiration dates.

"""
import pandas as pd
import yahoo_fin.stock_info as si
from functools import reduce

def get_company_info(tickers=list, db_constr=str) ->  dict:
    dict_data = {ticker: si.get_company_info(ticker) for ticker in tickers}
    return dict_data
def get_analysts_info(ticker=str) -> dict:
    """
    Scrapes data from the Analysts page for the input ticker from
    Yahoo Finance (e.g. https://finance.yahoo.com/quote/NFLX/analysts?p=NFLX.
    This includes information on earnings estimates, EPS trends / revisions etc.
    :param ticker:
    :return: Returns a dictionary containing the tables visible on the ‘Analysts’ page.
    """
    dict_analysts_info=si.get_analysts_info(ticker)
    return  dict_analysts_info

def get_data(tickers=list, start_date=str, end_date=str, index_as_date=True, interval='1d') -> pd.DataFrame:
    """
    :param start_date:
    :param end_date:
    :param index_as_date:
    :param interval:
    :param tickers is a list of Equity tickers:
    :return :  
    """

    try:
        price_data = {ticker: si.get_data(ticker.strip(), start_date, end_date, index_as_date, interval)
                      for ticker in tickers}
    except Exception as e:
        print("Yahoo_fin_Library.get_data (start_date={},end_date={}) failed to get history price : {}".format(start_date,end_date, str(e)))
        raise  e

    # DatetimeIndex: 10182  entries, 1980 - 12 - 12   to    2021 - 04 - 30


    combined = reduce(lambda x, y: x.append(y), price_data.values())
    # combined = reduce(lambda x, y: x.concat(y), price_data.values())
    return combined

def get_splits(tickers=list, start_date=str, end_date=str, index_as_date=True) -> pd.DataFrame:
    """
    :param start_date:
    :param end_date:
    :param index_as_date:
    :param tickers is a list of Equity tickers:
    :return :
    """

    try:
        # splits_data = {ticker: si.get_splits(ticker.strip(), start_date, end_date, index_as_date)
        #               for ticker in tickers}

        splits_data = {ticker: si.get_splits(ticker.strip()) for ticker in tickers}
    except Exception as e:
        print("Yahoo_fin_Library.get_splits (start_date={},end_date={}) failed to get split : {}".format(start_date,end_date, str(e)))
        raise  e

    combined = reduce(lambda x, y: x.append(y), splits_data.values())
    return combined

def get_balance_sheet(tickers=list, yearly=True):
    dict_data = {ticker: si.get_balance_sheet(ticker,yearly) for ticker in tickers}
    return dict_data

def get_income_statement(tickers=list, yearly=True):
    dict_data = {ticker: si.get_income_statement(ticker, yearly) for ticker in tickers}
    return dict_data

def get_cash_flow(tickers=list, yearly=True):
    dict_data = {ticker: si.get_cash_flow(ticker, yearly) for ticker in tickers}
    return dict_data



# balance_sheet = si.get_balance_sheet(ticker,yearly = False)
# print (balance_sheet)

if __name__ == '__main__':
    tickers=['AMZN']
    get_analysts_info('AMZN')
