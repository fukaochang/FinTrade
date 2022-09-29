import os
import datetime
import numpy as np
import pandas as pd
from Util import FileUtil, SystemEnv
from Instrument import SymbolPd
from DB import DBPrice
from MarketData import Yahoo_fin_Library
import yahoo_fin.stock_info as si
from yahoo_fin import news
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
    for i in range(len(df_data.columns)):
        print("Column i={}, {}".format(i, df_data.columns[i]))
    for index, row in df_data.iterrows():
        print("--------------------------index={}".format(index))
        for i in range(len(row)):
            print("i={}, {}".format(i, row[i]))
def display_dictionary( dict_data):
    for key, df_values in dict_data.items():
        print("--key = {}, value =  {}".format(key, df_values))
        # print("Category= {}-----".format(key))
        # print("columns = {}-----".format(df_values.columns))
        # for index, df_row in df_values.iterrows():
        #     print("--Index = {}, Item {}".format(index,df_row))
            # df_row.fillna(0, inplace=True)
            # for item, vlue in df_row[1:].items():
            #     print("Year={},Category = {}, item={} Values={}".format(item, df_row.index[0],df_row.iloc[0], vlue))

def get_news(ticker):
    """
    import feedparser

    yf_rss_url = 'https://feeds.finance.yahoo.com/rss/2.0/headline?s=%s&region=US&lang=en-US'
    feed = feedparser.parse(yf_rss_url % ticker)

    return feed.entries

    :param ticker:
    :return:
    """
    data = news.get_yf_rss(ticker)
    # print (type(data))

    for item in data:
        print("{}".format(item))
    # print(" Ticker {} , {}".format(ticker,data))

def get_earnings(tickers=list, db_constr=str):
    """
    NOTE : Finance’s financials page (Income Statemen, Balance sheet, Cash flow)
            Returns a dictionary with quarterly actual vs. estimated earnings per share,
                    quarterly revenue / earnings data, and yearly revenue / earnings data.
    :param tickers:
    :param db_constr:
    :return:
    """
    if not isinstance(tickers, list):
        raise TypeError

    try:
        dict_data = {ticker: si.get_earnings(ticker) for ticker in tickers}
        for ticker, dic_stmt in dict_data.items():
            # print("Ticker {}  ------------".format(ticker))
            for itemname, df in  dic_stmt.items():
                period = 'quarterly' if 'quarterly' in itemname else 'yearly'
                # print("\tPeriod {} = {} ------------".format(period, itemname ))
                for index, row in df.iterrows():
                    for i in range(1, len(row)):
                        if  period == 'quarterly':
                            quarter=row[0][2:]+row[0][0:2]
                            # print(quarter)
                        # print(" Date {} {}, {} = {}".format(df.columns[0],
                        #                             row[0] if period != 'quarterly' else row[0][2:]+row[0][0:2],
                        #                             df.columns[i], row[i]))
                        DBPrice.update_Revenue_Earnings_EPS(ticker,period
                                                            ,row[0] if period != 'quarterly' else row[0][2:]+row[0][0:2]
                                                            , df.columns[i], row[i], db_constr)

        print("Done...............")
    except Exception as e:
        print("Tickers {} : Invalid, get_earnings : {}".format(tickers, e))
        return

def get_earnings_for_date(earning_date,db_constr=str):
    """
    Note : Geta a list of ticker, its corresponding EPS estimate, and the time of the earnings release.
            on a specific Earning date
    Frequency : Daily
    :param earnings_date:
    :param db_constr:
    :return:
    """

    try:
        lst = si.get_earnings_for_date(earning_date)
        for dic in lst:
            if (dic['epsestimate'] is None) and  (dic['epsactual'] is None) and (dic['epssurprisepct'] is None):
                continue
            ticker =  dic['ticker']
            earningsdate = dic['startdatetime']
            for itemname, itemvalue in dic.items():
                if 'eps' in itemname:
                    # print("{} ,{}, {}, {}".format(ticker, earningsdate, itemname, itemvalue))
                    DBPrice.update_EPS_history(ticker, earningsdate, itemname, itemvalue, db_constr)


    except Exception as e:
        print("Tickers {} : Invalid,get_earnings_for_date : {}".format(tickers, e))
        return

def get_earnings_history(tickers=list, db_constr=str):
    """
    Note:  a list of dictionaries with QUARTERLY actual vs. estimated earnings per share
         along with dates of previous earnings releases.
    Frequecny : QUARTERLY
    :param tickers:
    :param db_constr:
    :return:
    """
    if not isinstance(tickers, list):
        raise TypeError

    try:
        dict_data = {ticker: si.get_earnings_history(ticker) for ticker in tickers}
        for ticker, lst in dict_data.items():
            print("Ticker {} , type = ------------".format(ticker,type(lst)))
            for dic_values in lst:
                earningsdate =  dic_values['startdatetime']
                for itemname,itemvalue in  dic_values.items():
                    if 'eps' in itemname:
                        print("{} ,{}, {}, {}".format(ticker,earningsdate, itemname, itemvalue))
                        DBPrice.update_EPS_history(ticker, earningsdate, itemname, itemvalue , db_constr)

    except Exception as e:
        print("Tickers {} : Invalid, get_earnings_history : {}".format(tickers, e))
        return
def get_next_earnings_date(tickers=list, db_constr=str):
    """
    Note: Next Earning Date for a given ticker


    :param tickers:
    :param db_constr:
    :return:
    """
    if not isinstance(tickers, list):
        raise TypeError

    try:
        dict_data = {ticker: si.get_next_earnings_date(ticker) for ticker in tickers}

        for ticker, earningsDt in dict_data.items():
            # print("Ticker={} Next Earning Date {} ------------".format(ticker,earningsDt.strftime("%m/%d/%Y")))
            DBPrice.update_next_earnings_date(ticker,earningsDt.strftime("%m/%d/%Y"),db_constr )
    except Exception as e:
        print("Tickers {} : Invalid, get_quote_data : {}".format(tickers, e))
        return
def get_quote_table(tickers=list, db_constr=str):
    """
    Yahoo Finance Summary - Frequencey : Intraday (17)
                1y target est
                52 week range
                Previous close,
                Open,
                Ask
                Bid
                Quote price
                Volume
                Avg. volume
                Beta (5y monthly)
                Day's range
                PE ratio (ttm)
                EPS (ttm)
                Earnings date
                Ex-dividend date
                Forward dividend & yield
                Market cap

    :param tickers:
    :param db_constr:
    :return:
    """
    if not isinstance(tickers, list):
        raise TypeError

    try:
        dict_data = {ticker: si.get_quote_table(ticker,True) for ticker in tickers}

        for ticker, dic in dict_data.items():
            # print("Ticker {} ------------".format(ticker))
            for key, df_values in dic.items():
                # print("{}, {}, type={}".format(key.capitalize(), df_values, type(df_values)))
                DBPrice.update_daily_quote_table(ticker, key.capitalize(), df_values, db_constr)

    except Exception as e:
        print("Tickers {} : Invalid, get_quote_data : {}".format(tickers, e))
        return


def get_quote_data(tickers=list, db_constr=str):
    """
    returns a collection of useful data on a stock ticker.
    It returns a dictionary containing over 70 elements, including current
        real-time price,
        company name,
        book value,
        P/E,
        Current Market State (Open / Closed),
        Pre-Market Price (if applicable),
        Intraday Price
         50-day average,
         200-day average,
        Post-Market Price (if applicable), and more.
         shares outstanding, and more.
    :param tickers:
    :param db_constr:
    :return:
    """
    if not isinstance(tickers, list):
        raise TypeError

    try:
        dict_data = {ticker: si.get_quote_data(ticker) for ticker in tickers}

        for ticker, dic in dict_data.items():
            print("Ticker {} ------------".format(ticker))
            for key, value in dic.items():
                print("{}, {}".format(key.capitalize(), value))
                DBPrice.update_daily_quote(ticker, key.capitalize(),value, db_constr)

    except Exception as e:
        print("Tickers {} : Invalid, get_quote_data : {}".format(tickers, e))
        return

def get_analysts_info(tickers=list,db_constr=str) -> dict:
    """
       Scrapes data from the Analysts page for the input ticker from
       Yahoo Finance (e.g. https://finance.yahoo.com/quote/NFLX/analysts?p=NFLX.
       This includes information on Earnings estimates, EPS trends / revisions etc.
       :param ticker:
       :param db_constr:
       :return: Returns a dictionary containing the tables visible on the ‘Analysts’ page.
    """
    # dict_analysts_info=si.get_analysts_info(ticker)
    # display_dictionary(dict_analysts_info)
    # DBPrice.update_analysts_info(ticker, dict_analysts_info, db_constr)

    if not isinstance(tickers, list):
        print("Type Error")
        raise TypeError

    try:
        dict_data = {ticker: si.get_analysts_info(ticker) for ticker in tickers}

        for ticker, df in dict_data.items():
             DBPrice.update_analysts_info(ticker, df, db_constr)

    except Exception as e:
        print("Tickers {} : Invalid, get_stats_valuation : {}".format(tickers, e))
        return

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

"""
Finance Yahoo! Statistics Section
    The most important place on Yahoo! Finance to do our fundamental analysis is the Statistics section.
    A lot of numbers here were calculated from numbers in the income statement, balance sheet and cash flow. 
    So the Statistics section covers basic information from these there statements but is much easier 
    to look at and analyse.
    
"""
def get_stats_valuation(tickers=list, db_constr=str):
    """
     Frequency - Daily
    Valuation Measures: help us decide on how much this stock “really” worth based on how well the company does it business.
        Market Cap (intraday): the current market price x the number of shares outstanding
        Enterprise Value (EV) : a company’s total value. This number can be understood as the money a buyer needs to pay
                                if this company is for sale. How much does the new owner have to pay in an acquisition?
                                The buyer not only needs to pay the equity value (roughly the value of market cap),
                                but also needs to repay the company’s debts (and keeps its cash if there is any).
                                *****  Market Cap + Total Debt - (cash +investments)
                                EV is believed to be a better indicator than market cap because it also takes a
                                company’s capital structure into consideration
        Trailing P/E: how much investors are willing to pay for a claim to one dollar of a company’s earnings.
                                ***** Market Value per Share / Earnings per Share.(EPS)
                                When you calculate P/E, for the number of earnings(EPS):
                                - Trailing P/E :  the net earnings over the last 12 months, which gives you the Trailing P/E,
                                - Forward P/E : the estimated net earnings over next 12 months
                                Compare P/E ratios between two companies in the same industry
                                    (cross-industry comparison is not very useful) The lower , The Better
                                the P/E ratio could be negative

        Forward P/E:  ** Market Value per Share / Estimated Earning (over next 12 months) per Share.
                        Note that while Trailing P/E is calculated using real historical data,
                        Forward P/E is calculated using estimated earning data provided by financial analysts.
                        As analyst’s estimates tend to be over-optimistic,forward P/E number might not be that reliable.
                        Roughly speaking, when you compare a company’s trailing P/E to its forward P/E,
                        it’s a good sign that the forward P/E is lower than the trailing P/E,
                         because that indicates this company’s earning is likely increase next year.
        PEG Ratio: a stocks’s P/E ratio divided by the growth rate of its earnings for a period of time
                    (5 years expected earnings in Yahoo! Finance in this example).
                    P/E doesn’t consider a company’s growth rate. However, the PEG ratio does.
                    PEG is quite accurate for growth companies, it gives inaccurate results for dividend stocks.
                    What is considered a good PEG ratio? Well, some investor believes that positive PEG ratios
                    lower than 1 are attractive (suggesting that stock is undervalued)
                    and a PEG ratio of 2 or more indicates a stock is overvalued.



        Returned Data Structure--
                Column ( Dates....)
       +-------+----+-----+----+
        (ItemName)
    :param tickers:
    :param db_constr:
    :return:
    """
    if not isinstance(tickers, list):
        print("Type Error")
        raise TypeError

    try:
        dict_data = {ticker: si.get_stats_valuation(ticker) for ticker in tickers}

        for ticker, df in dict_data.items():

            # len_col = len(df.columns)
            # for i in range(len_col):
            #     print("column = {}".format(df.columns[i]))
            # print(df.index.values)
            for index, row in df.iterrows():
                if len(row) <= 0 :
                    print("Tciker ={}".format(ticker))
                    continue

                for i in range(1, len(row)):
                    # print("date ='{}' , itemname={}, itemvalue={}".format(df.columns[i].replace('As of Date:','')\
                    #                                                     .replace('Current','').strip(), row[0],row[i]))
                    DBPrice.update_stats_valuation(ticker, df.columns[i].replace('As of Date:','').replace('Current','').strip(),\
                                         row[0],row[i], db_constr)

                # print("{} {} {}".format( index, columns[1],row[0], row[1]))
            #     DBPrice.update_stats(ticker, MostRecentQuarter, row['Attribute'], row['Value'], db_constr)
            # df_t = df.T
            # print (df_t)
    except Exception as e:
        print("Tickers {} : Invalid, get_stats_valuation : {}".format(tickers, e))
        return

def get_stats(tickers=list, db_constr=str):
    """
        Frequency - Quarter
        moving averages, return on equity, shares outstanding, etc
    :param tickers:
    :param db_constr:
    :return:
    """
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

# def get_dividends()
    """
    Downloads historical dividend data of a stock into a pandas data frame.
    """
# def get_splits()

def get_financials(tickers, yearly=True, quarterly=True, db_constr=str, db_upd=True, output_file=False):
    """
        Note: more efficiently extract balance sheets, cash flows, and income statements for the same ticker
                all at once
                Returns a dictionary with the following keys:

                If yearly = True:

                yearly_income_statement
                yearly_balance_sheet
                yearly_cash_flow


                If quarterly = True:

                quarterly_income_statement
                quarterly_balance_sheet
                quarterly_cash_flow
    :return:
    """
    dict_data = si.get_financials(tickers,yearly,quarterly)
    for key, df_values in dict_data.items():
        print("------key = {}, value =  {} col = {}".format(key, df_values.index, df_values.columns ))
        # display_dataframe(df_values)
            # print("Category= {}-----".format(key))
            # print("columns = {}-----".format(df_values.columns))
            # for index, df_row in df_values.iterrows():
            #     print("--Index = {}, Item {}".format(index,df_row))
                # df_row.fillna(0, inplace=True)
                # for item, vlue in df_row[1:].items():
                #     print("Year={},Category = {}, item={} Values={}".format(item, df_row.index[0],df_row.iloc[0], vlue))


def get_balance_sheet(tickers, yearly=True, db_constr=str, db_upd=True, output_file=False) -> dict:
    """
    Yahoo Finace Financials -a company’s staying power (Does it have enough cash? Too much debt?).
    :param tickers:
    :param yearly:
    :param db_constr:
    :param db_upd:
    :param output_file:
    :return:
    """
    dict_balance_sheet = Yahoo_fin_Library.get_balance_sheet(tickers,yearly)

    if db_upd :
        DBPrice.update_balance_sheet(dict_balance_sheet,db_constr)

    if output_file:
        for ticker, df_sheet in dict_balance_sheet.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_balance_sheet.csv")
            df_sheet.to_csv(out_file)
    return dict_balance_sheet

def get_income_statement(tickers, yearly=True,db_constr=str,  db_upd=True, output_file=False)-> dict:
    """
    Yahoo Finace Financials - a company’s profitability (Does it make money or loss money?).
    :param tickers:
    :param yearly:
    :param db_constr:
    :param db_upd:
    :param output_file:
    :return:
    """
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
    """
    Yahoo Finace Financials - the detailed in and out flow of a company’s cash
        (Seeing real cash coming in is more convincing than what’s claimed in the income statement).
    :param tickers:
    :param yearly:
    :param db_constr:
    :param db_upd:
    :param output_file:
    :return:
    """
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