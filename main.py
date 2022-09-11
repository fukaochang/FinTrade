import os
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


    ls_tickers =[ 'MSTR', 'NCTy', 'OPEN', 'BLNK','SPY', 'TSLA', 'NVDA','MSFT', 'BABA', 'GOOG', 'GOOGL','META','NIO','PDD', 'RIVIN','V','NVDA','SPY']
    ls_tickers = ['META', 'NIO', 'PDD', 'RIVIN', 'V', 'NVDA']
    ls_tickers = ['AMZN']
    Yahoo_FinanceData.get_stats(ls_tickers, SystemEnv.g_globaldb_constr)



    # the following functions completely tests

    # Yahoo_FinanceData.get_company_info(ls_tickers, SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_analysts_info('AMZN', SystemEnv.g_globaldb_constr)
    # Yahoo_FinanceData.get_balance_sheet(ls_tickers, yearly, SystemEnv.g_globaldb_constr, db_upd, output_file)
    #Yahoo_FinanceData.get_income_statement(ls_tickers, yearly, SystemEnv.g_globaldb_constr, db_upd, output_file)
    # Yahoo_FinanceData.get_cash_flow(ls_tickers, yearly,  SystemEnv.g_globaldb_constr, db_upd, output_file)

def main():

    SystemEnv.read_config('.\config.ini')
    # print("  SystemEnv.read_config('.\config.ini')..........")

    unit_test_yahoo_fundamental_data()

    # the following functions completely tests
    # unit_test__configure_ini()
    # unit_test__database()
    # unit_test_yahoo_finance()
    # unit_test_yahoo_finance_atr()
    # unit_test_yahoo_fundamental_data()

if __name__ == '__main__':
    main()

