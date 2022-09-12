import os
import numpy as np
import pandas as pd
from Util import FileUtil, SystemEnv
from Instrument import SymbolPd
from DB import DBPrice
from MarketData import Yahoo_fin_Library
import yahoo_fin.stock_info as si
from functools import reduce

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
def display_dataframe ( df_data ):
    for index, row in df_data.iterrows():
        print("index={}, row={}".format(index,row['Value']))
def display_dictionary( dict_data):
    for key, df_values in dict_data.items():
        # print("Category= {}-----".format(key))
        # print("columns = {}-----".format(df_values.columns))
        for index, df_row in df_values.iterrows():
            # print("--Category = {}, Item {}".format(df_row.index[0],df_row.iloc[0]))
            df_row.fillna(0, inplace=True)
            for item, vlue in df_row[1:].items():
                print("Year={},Category = {}, item={} Values={}".format(item, df_row.index[0],df_row.iloc[0], vlue))

def get_analysts_info(ticker=str,db_constr=str) -> dict:
    """
       Scrapes data from the Analysts page for the input ticker from
       Yahoo Finance (e.g. https://finance.yahoo.com/quote/NFLX/analysts?p=NFLX.
       This includes information on earnings estimates, EPS trends / revisions etc.
       :param ticker:
       :param db_constr:
       :return: Returns a dictionary containing the tables visible on the ‘Analysts’ page.
    """
    dict_analysts_info=si.get_analysts_info(ticker)
    # display_dictionary(dict_analysts_info)
    DBPrice.update_analysts_info(ticker, dict_analysts_info, db_constr)

def get_company_info(tickers=list, db_constr=str):
    """
    Invalid Ticker will cause Exception
    :param tickers:
    :param db_constr:
    :return:
    """
    if not isinstance(tickers, list):
        raise TypeError
    try:
        dict_data = {ticker: si.get_company_info(ticker) for ticker in tickers}
    except Exception as e:
        print("Tickers {} : Invalid, get_company_info : {}".format(tickers, e))
        return

    if isinstance(tickers, (list, tuple, pd.Series, pd.Index)):
        for ticker,df in dict_data.items():
            print("Ticker ={}".format(ticker))
            # print(type(df))
            # print(df.index)
            # print(df.index.isin(['sector', 'industry', 'website']).tolist())
            # print(df.index.isin(['sector', 'industry', 'website']).any())
            if df.index.isin(['sector', 'industry', 'website']).any():
                # for index, row in df.iterrows():
                #     print("index={}, value={}".format(index, row['Value']))
                sector = df.loc['sector','Value']
                industry = df.loc['industry','Value']
                website =  df.loc['website','Value']
                print("Ticker ={}, Sector={}, Industry={}, WebSite={}".format(ticker,sector,industry,website) )
                DBPrice.update_company_info(ticker.upper(),sector,industry,website,db_constr)
            else:
                print("Ticker ={} does not have the company information".format(ticker))

def get_stats_valuation(tickers=list, db_constr=str):
    if not isinstance(tickers, list):
        print("Type Error")
        raise TypeError

    try:
        dict_data = {ticker: si.get_stats_valuation(ticker) for ticker in tickers}

        for ticker, df in dict_data.items():
            # df = df.T
            # print(type(df))
            # print(df.columns)
            len_col = len(df.columns)
            for i in range(len_col):
                print("column = {}".format(df.columns[i]))
            # print(df.index.values)
            for index, row in df.iterrows():
                # print(type(row))
                print("index = {}, itemname={}".format( index, row[0] ) )

                for i in range(len(row)):
                    print("date ={} , itemname={}, itemvalue={}".format(df.columns[i], row[0],row[i]))

                # print("{} {} {}".format( index, columns[1],row[0], row[1]))
            #     DBPrice.update_stats(ticker, MostRecentQuarter, row['Attribute'], row['Value'], db_constr)
            # df_t = df.T
            # print (df_t)
    except Exception as e:
        print("Tickers {} : Invalid, get_stats_valuation : {}".format(tickers, e))
        return

def get_stats(tickers=list, db_constr=str):
    if not isinstance(tickers, list):
        raise TypeError

    try:
        dict_data = {ticker: si.get_stats(ticker) for ticker in tickers}

        date_Fiscal_Year_Ends ='Fiscal Year Ends'
        date_Last_Split_Date ='Last Split Date 3'
        date_Most_Recent_Quarter = 'Most Recent Quarter (mrq)'

        for ticker, df in dict_data.items():

            FiscalYearEnds = df.loc[df['Attribute'] == date_Fiscal_Year_Ends]['Value'].iloc[0]
            print("Fiscal Year Ends = {}".format(  FiscalYearEnds))
            LastSplitDate = df.loc[df['Attribute'] == date_Last_Split_Date]['Value'].iloc[0]
            print("Last_Split_Date = {}".format(LastSplitDate))
            MostRecentQuarter = df.loc[df['Attribute'] == date_Most_Recent_Quarter]['Value'].iloc[0]
            print("Most Recent Quarter = {}".format(MostRecentQuarter))

            for index, row in df.iterrows():
                # print("item ={}, value =  {}".format(row['Attribute'], row['Value']))
                DBPrice.update_stats(ticker,MostRecentQuarter, row['Attribute'],row['Value'], db_constr)

    except Exception as e:
        print("Tickers {} : Invalid, get_stats : {}".format(tickers, e))
        return



        # print(df.index.isin(['sector', 'industry', 'website']).tolist())
        # print(df.index.isin(['sector', 'industry', 'website']).any())
        # if df.index.isin(['sector', 'industry', 'website']).any():
        #     # for index, row in df.iterrows():
        #     #     print("index={}, value={}".format(index, row['Value']))
        #     sector = df.loc['sector', 'Value']
        #     industry = df.loc['industry', 'Value']
        #     website = df.loc['website', 'Value']
        #     print("Ticker ={}, Sector={}, Industry={}, WebSite={}".format(ticker, sector, industry, website))
        #     DBPrice.update_company_info(ticker.upper(), sector, industry, website, db_constr)
        # else:
        #     print("Ticker ={} does not have the company information".format(ticker))


def get_historical_price(tickers=list,start_date=str, end_date=str, db_constr=str, db_upd=True, output_file=False)->pd.DataFrame:
    """

      :param tickers:
      :param start_date:
      :param end_date:
      :param db_constr : ConnectionString
      :param db_upd:
      :param output_file:
      :return: pd.DataFrame
    """
    df_price = Yahoo_fin_Library.get_data(tickers,start_date, end_date, False)

    if isinstance(tickers, (list, tuple, pd.Series, pd.Index)):
        ticker_group = df_price.groupby('ticker')
        # print( type(ticker_group)) #<class 'pandas.core.groupby.generic.DataFrameGroupBy'>

        for name, grp in ticker_group:
            df_price = SymbolPd.calc_atr(grp)
            # print(df_price.info())
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


def get_balance_sheet(tickers, yearly=True, db_constr=str, db_upd=True, output_file=False) -> dict:

    dict_balance_sheet = Yahoo_fin_Library.get_balance_sheet(tickers,yearly)

    if db_upd :
        DBPrice.update_balance_sheet(dict_balance_sheet,db_constr)

    if output_file:
        for ticker, df_sheet in dict_balance_sheet.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_balance_sheet.csv")
            df_sheet.to_csv(out_file)
    return dict_balance_sheet

def get_income_statement(tickers, yearly=True,db_constr=str,  db_upd=True, output_file=False)-> dict:

    dict_income_statement = Yahoo_fin_Library.get_income_statement(tickers,yearly)

    print(dict_income_statement)

    if db_upd :
        DBPrice.update_income_statement(dict_income_statement,db_constr)

    if output_file:
        for ticker, df_sheet in dict_income_statement.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_income_statement.csv")
            df_sheet.to_csv(out_file)
    return dict_income_statement

def get_cash_flow(tickers, yearly=True, db_constr=str, db_upd=True, output_file=False)-> dict:

    dict_cash_flow = Yahoo_fin_Library.get_cash_flow(tickers,yearly)

    if db_upd :
        DBPrice.update_cash_flow(dict_cash_flow,db_constr)

    if output_file:
        for ticker, df_sheet in dict_cash_flow.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_cash_flow.csv")
            df_sheet.to_csv(out_file)

    return dict_cash_flow
def market_data(tickers):
    yearly=False
    db_upd=True
    output_file=True
    get_historical_price(['AAPL',], db_upd, output_file)
    get_balance_sheet(tickers, yearly, db_upd, output_file)
    get_income_statement(tickers, yearly, db_upd,output_file)
    get_cash_flow(tickers, yearly, db_upd,output_file)

def main():

    # SystemEnv.read_config('c:\Temp\jquant\config.ini')
    # tickers = (SystemEnv.g_tick_list[SystemEnv.ConfigSection.E_TICKER.value])

    #-------------------------------------------------------------------------
    #   1. Importing the market data from Yahoo Finance and save them to MySql
    #--------------------------------------------------------------------------
    # get_historical_price(tickers)
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

if __name__ == '__main__':
    tickers=['AMZN','AAPL','SQQQ','BA','BRK-B','ABNB','STEM','MSTR','NCT','OPEN','BLNK','TSLA','NVDA','MSFT','BABA','GOOG','GOOGL','SPY']
    # get_analysts_info('AMZN'),BLNK
    # get_company_info(tickers)
    get_company_info('AMZN',"dsfas")