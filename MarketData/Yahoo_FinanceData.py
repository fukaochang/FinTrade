import os
import pandas as pd
from Util import FileUtil, SystemEnv
from Instrument import SymbolPd
from DB import DBPrice
from MarketData import yahoo_fin_Market_Data

def read_price_file():
    """
        price file directory : SystemEnv.read_config('config.ini')
        :return:
    """
    # SystemEnv.read_config('config.ini')
    # print(SystemEnv.g_price_file)

    for key, value in SystemEnv.g_price_file.items():
        print('{}={}'.format(key, value))

    price_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], "Yahoo_TSLA.csv")

    if not FileUtil.file_exist(price_file):
        print("Price File {} does not exist.".format(price_file))
        return
    df_price = pd.read_csv(price_file)

    return df_price


"""
Scraping Financials
"""


def get_historical_price(tickers=list,start_date=str, end_date=str, db_constr=str, db_upd=True, output_file=False)->pd.DataFrame:
    """

      :param tickers:
      :param start_date:
      :param end_date:
      :param db_upd:
      :param output_file:
      :return: pd.DataFrame
    """
    df_price = yahoo_fin_Market_Data.get_data(tickers,start_date, end_date, False)
    print(df_price.info())

    if isinstance(tickers, (list, tuple, pd.Series, pd.Index)):
        ticker_group = df_price.groupby('ticker')
        # print( type(ticker_group)) #<class 'pandas.core.groupby.generic.DataFrameGroupBy'>

        for name, grp in ticker_group:
            df_price = SymbolPd.calc_atr(grp)
            print(df_price.info())
            # if output_file :
            #     price_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], name+"_historical_price.csv")
            #     df_price.to_csv(price_file)
            if db_upd :
                DBPrice.update_price_atr(df_price, db_constr)
    else:
        df_price = SymbolPd.calc_atr(df_price)
        # if output_file:
        #     price_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], tickers + "_historical_price.csv")
        #     df_price.to_csv(price_file)
        if db_upd:
            DBPrice.update_price_atr(df_price, db_constr)

    return df_price


def get_balance_sheet(tickers, yearly=True,  db_upd=True, output_file=False):

    dict_balance_sheet = yahoo_fin_Market_Data.get_balance_sheet(tickers,yearly)

    if db_upd :
        DBPrice.update_balance_sheet(dict_balance_sheet)

    if output_file:
        for ticker, df_sheet in dict_balance_sheet.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_balance_sheet.csv")
            df_sheet.to_csv(out_file)


def get_income_statement(tickers, yearly=True,  db_upd=True, output_file=False):

    dict_income_statement = yahoo_fin_Market_Data.get_income_statement(tickers,yearly)

    print(dict_income_statement)

    if db_upd :
        DBPrice.update_income_statement(dict_income_statement)

    if output_file:
        for ticker, df_sheet in dict_income_statement.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_income_statement.csv")
            df_sheet.to_csv(out_file)


def get_cash_flow(tickers, yearly=True,  db_upd=True, output_file=False):

    dict_cash_flow = yahoo_fin_Market_Data.get_cash_flow(tickers,yearly)

    if db_upd :
        DBPrice.update_cash_flow(dict_cash_flow)

    if output_file:
        for ticker, df_sheet in dict_cash_flow.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_cash_flow.csv")
            df_sheet.to_csv(out_file)


def market_data(tickers):
    yearly=False
    db_upd=True
    output_file=True
    get_historical_price(['AAPL',], db_upd, output_file)
    get_balance_sheet(tickers, yearly, db_upd, output_file)
    get_income_statement(tickers, yearly, db_upd,output_file)
    get_cash_flow(tickers, yearly, db_upd,output_file)

def main():

    SystemEnv.read_config('c:\Temp\jquant\config.ini')
    tickers = (SystemEnv.g_tick_list[SystemEnv.ConfigSection.E_TICKER.value])

    #-------------------------------------------------------------------------
    #   1. Importing the market data from Yahoo Finance and save them to MySql
    #--------------------------------------------------------------------------
    get_historical_price(tickers)
    # market_data(tickers)

    # -------------------------------------------
    #   2. Geting the symbols Time Sereres from MySql
    # -------------------------------------------
    # SymbolTimeSeries.get_price(tickers)

    # -------------------------------------------
    #   3. BackTest a given Symbol
    # -------------------------------------------
    # stock_trade_days = StockTradeDays.StockTradeDays('TSLA')
    # print(stock_trade_days.get_price())

    print("Done.................")
