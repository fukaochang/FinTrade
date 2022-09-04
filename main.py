import os
from Util import  SystemEnv
from DB import  dbconnection, DBPrice


def configure_ini():
    tickers = (SystemEnv.g_tick_list[SystemEnv.ConfigSection.E_TICKER.value])
    print( tickers)
    print(SystemEnv.g_mssql_connection)
    print(SystemEnv.g_mssql_connection['connectionstring'])
    print("test_configure_ini - Done !")

def database():
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
def main():
    SystemEnv.read_config('.\config.ini')
    # configure_ini()
    database()

if __name__ == '__main__':
    main()

