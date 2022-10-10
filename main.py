import os
import argparse
import datetime
import pandas as pd
from Util import  SystemEnv
from DB import  dbconnection, DBPrice
from MarketData import Yahoo_fin_Library,Yahoo_FinanceData
from Instrument import  SymbolPd
import warnings

warnings.filterwarnings("ignore")

def unit_test_configure_ini():
    tickers = (SystemEnv.g_tick_list[SystemEnv.ConfigSection.E_TICKER.value])
    print(tickers)
    print(SystemEnv.g_mssql_connection)
    print(SystemEnv.g_mssql_connection['connectionstring'])
    print("test_configure_ini - Done !")

def unit_test_database():
    constr = SystemEnv.g_globaldb_constr

    # stored proc without parameter
    proc = "dbo.usp_Securitymaster_Sel"
    df = dbconnection.f_pyodbc_Sel(proc, constr)
    print(df)

    # stored proc with parameter
    proc = "dbo.usp_Securitymaster_Sel  @ticker='{0}'".format('AAPL')
    df1 = dbconnection.f_pyodbc_Sel(proc, constr)
    print(df1)

    print("database - Done !")

def unit_test_yahoo_finance():
    globaldb = SystemEnv.g_globaldb_constr

    # stored proc with parameter - get the tickers
    proc = "dbo.usp_Securitymaster_Sel  @ticker='{0}'".format('AMZN')
    df_tickers = dbconnection.f_pyodbc_Sel(proc, globaldb)
    ls_tickers = df_tickers['Ticker'].tolist()
    # calling yahoo finance
    ls_tickers=['AMZN', 'AAPL']
    start_date = '2022-01-01'
    end_date = '2022-08-31'
    df_price = Yahoo_fin_Library.get_data(ls_tickers, start_date, end_date, index_as_date=False, interval='1d')

    # print(df_price.info())
    print( df_price)

    DBPrice.update_price(df_price,globaldb )

    print ("unit_test_yahoo_finance Done.")

def unit_test_yahoo_finance_atr():
    globaldb = SystemEnv.g_globaldb_constr

    # stored proc with parameter - get the tickers
    proc = "dbo.usp_Securitymaster_Sel  @ticker='{0}'".format('AMZN')
    df_tickers = dbconnection.f_pyodbc_Sel(proc, globaldb)
    ls_tickers = df_tickers['Ticker'].tolist()
    # calling yahoo finance
    ls_tickers = ['AMZN', 'AAPL']
    start_date = '2022-01-01'
    end_date = '2022-08-31'
    df_price = Yahoo_FinanceData.get_historical_price(ls_tickers,start_date,end_date, globaldb,True, False)
    # print (df_price)

    print("unit_test_yahoo_finance_atr Done.")

def unit_test_yahoo_fundamental_data():
    yearly = False
    db_upd = True
    output_file = True
    # ls_tickers = ['BRK-B']


    # ls_tickers_SPY =[ 'MSTR', 'NCTy', 'OPEN', 'BLNK', 'TSLA', 'NVDA','MSFT', 'BABA', 'GOOG', 'GOOGL','META','NIO','PDD', 'RIVN','V','NVDA','SPY']
    # ls_tickers = ['META', 'NIO', 'PDD', 'RIVIN', 'V', 'NVDA']

    ls_tickers =['AMZN', 'AAPL', 'MSTR', 'NCTY', 'OPEN', 'BLNK','TSLA', 'NVDA','MSFT', 'BABA', 'GOOG', 'GOOGL','META','NIO','PDD', 'RIVN','V','NVDA']

    # ls_tickers = ['AMZN','TSLA']


    # -------------------------------------------------------------
    # the following functions completely tests
    # ---------------------------------------------------------------
    Yahoo_FinanceData.get_news('AMZN')

    # Yahoo_FinanceData.get_financials('AMZN', True, False)
    # Yahoo_FinanceData.get_balance_sheet(ls_tickers, yearly, SystemEnv.g_globaldb_constr, db_upd, output_file)
    #Yahoo_FinanceData.get_income_statement(ls_tickers, yearly, SystemEnv.g_globaldb_constr, db_upd, output_file)
    # Yahoo_FinanceData.get_cash_flow(ls_tickers, yearly,  SystemEnv.g_globaldb_constr, db_upd, output_file)

    # Yahoo_FinanceData.get_analysts_info(ls_tickers, SystemEnv.g_globaldb_constr)

    # Yahoo_FinanceData.get_earnings(ls_tickers, SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_earnings_for_date('09/20/2022', SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_earnings_history(ls_tickers, SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_next_earnings_date(ls_tickers, SystemEnv.g_globaldb_constr)

    # Yahoo_FinanceData.get_stats_valuation(ls_tickers, SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_stats(ls_tickers, SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_quote_table(ls_tickers, SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_company_info(ls_tickers, SystemEnv.g_globaldb_constr)
def main(opt1=str,opt2=None,opt3=None,opt4=None):

    print("opt1={}, opt2={}".format(opt1,opt2))
    # SystemEnv.read_config('.\config.ini')
    # print("  SystemEnv.read_config('.\config.ini')..........")

    # ls_tickers = ['AMZN', 'AAPL', 'MSTR', 'NCTY', 'OPEN', 'BLNK', 'TSLA', 'NVDA', 'MSFT', 'BABA', 'GOOG', 'GOOGL',
    #               'META', 'NIO', 'PDD', 'RIVN', 'V', 'NVDA']

    # ls_tickers=['^GSPC']
    ls_tickers = ['^IXIC']
    if opt1 == "company_info":
        Yahoo_FinanceData.get_company_info(opt2, SystemEnv.g_globaldb_constr)
    elif opt1 == "quote_table":
        Yahoo_FinanceData.get_quote_table(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "historical_price_year":
        Yahoo_FinanceData.get_historical_price_year(ls_tickers,opt2,opt3)
    elif opt1 == "get_stats":
        Yahoo_FinanceData.get_stats(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "stats_valuation":
        Yahoo_FinanceData.get_stats_valuation(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "get_next_earnings_date":
        Yahoo_FinanceData.get_next_earnings_date(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "get_next_earnings_date":
        Yahoo_FinanceData.get_next_earnings_date(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "get_earnings_history":
        Yahoo_FinanceData.get_earnings_history(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "get_earnings_history":
        Yahoo_FinanceData.get_earnings_history(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "earnings_for_date":
        Yahoo_FinanceData.get_earnings_for_date(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "analysts_info":
        Yahoo_FinanceData.get_earnings_for_date(ls_tickers, SystemEnv.g_globaldb_constr)
    elif opt1 == "get_splits":
        Yahoo_FinanceData.get_splits( opt2)
    elif opt1 == "get_dividends":
        Yahoo_FinanceData.get_dividends( opt2)
    elif opt1 == "news":
        Yahoo_FinanceData.get_news(opt2)
    else:
        print("no option")

    # unit_test_yahoo_fundamental_data()

    # the following functions completely tests
    # unit_test__configure_ini()
    # unit_test__database()
    # unit_test_yahoo_finance()
    # unit_test_yahoo_finance_atr()
    # unit_test_yahoo_fundamental_data()

if __name__ == '__main__':
    SystemEnv.read_config('.\config.ini')


    if __debug__:
        # opt1="news"
        # opt2="AMZN"

        # opt1 = "historical_price_year"
        opt1 = "get_splits"
        opt2 = ['AMZN', 'AAPL', 'MSTR', 'NCTY', 'OPEN', 'BLNK', 'TSLA', 'NVDA', 'MSFT', 'BABA', 'GOOG', 'GOOGL',
                      'META', 'NIO', 'PDD', 'RIVN', 'V', 'NVDA']
        opt1 = "get_dividends"

        main(opt1, opt2)

        # opt1="company_info"
        # opt2= ['AMZN', 'AAPL', 'MSTR', 'NCTY', 'OPEN', 'BLNK', 'TSLA', 'NVDA', 'MSFT', 'BABA', 'GOOG', 'GOOGL',
        #             'META', 'NIO', 'PDD', 'RIVN', 'V', 'NVDA']
        #
        # main(opt1, opt2)

    else:
        parser = argparse.ArgumentParser()
        # Add a required, positional argument
        parser.add_argument("opt1",
                            choices=["company_info", "quote_table", "historical_price_year","get_stats", "stats_valuation",
                                    "next_earnings_date","earnings_history", "earnings_for_date","earnings","get_splits",
                                    "get_dividends", "analysts_info", "financials","news"],
                            type=str, help="Your name")
        parser.add_argument("opt2")

        args = parser.parse_args()
        print("opt1={}, opt2={}".format(args.opt1, args.opt2))
        main(args.opt1, args.opt2)

