import os
import pandas as pd
from Util import  SystemEnv
from DB import  dbconnection, DBPrice
from MarketData import yahoo_fin_Market_Data
from Instrument import  SymbolPd

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
    df_price = yahoo_fin_Market_Data.get_data(ls_tickers, start_date, end_date, index_as_date=False, interval='1d')

    # print(df_price.info())
    print( df_price)

    DBPrice.update_price(df_price,globaldb )

    print ("unit_test_yahoo_finance Done.")

def main():
    SystemEnv.read_config('.\config.ini')
    # unit_test__configure_ini()
    # unit_test__database()
    unit_test_yahoo_finance()

if __name__ == '__main__':
    main()

