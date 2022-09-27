import string
from datetime import datetime
import pandas as pd
import pyodbc
from Util import  SystemEnv


def update_Revenue_Earnings_EPS(ticker,period ,yrQtr , itemname, itemvalue , constr=str):
    """
    Table Revenue_Earnings_EPS - (Year or Year Quarter, Revenue, Earnings,, EPS_Actual,EPS_Estimate)
    :param ticker:
    :param earningsdate:
    :param itemname:
    :param itemvalue:
    :param constr:
    :return:
    """
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.[usp_Revenue_Earnings_EPS_IU] '{}','{}','{}','{}', {} ".format(ticker,period, yrQtr,itemname,itemvalue )
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("{} ,{}, {}, {}, {}".format(ticker,period, yrQtr,itemname,itemvalue ))
        print("Failed to execute stored procedure update_Revenue_Earnings_EPS : {}".format(e))
    finally:
        cursor.close()
        conn.close()

def update_EPS_history(ticker, earningsdate, itemname, itemvalue , constr=str):
    """
    Table Earnings_history_pivot - (EarningsDate,EPS_Estimate, EPS_Actual,EPS_Surprise_Pct)
    :param ticker:
    :param earningsdate:
    :param itemname:
    :param itemvalue:
    :param constr:
    :return:
    """
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.[usp_EPS_History_IU] '{}','{}','{}','{}' ".format(ticker, earningsdate,itemname,itemvalue )
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("{} ,{}, {}, {}".format(ticker, earningsdate, itemname, itemvalue))
        print("Failed to execute stored procedure update_EPS_history : {}".format(e))
    finally:
        cursor.close()
        conn.close()


def update_next_earnings_date(ticker, earningsdate, constr=str):
    """
    Table Earnings_Date(Ticker, Earning_Date)
    :param ticker:
    :param earningsdate:
    :param constr:
    :return:
    """
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.[usp_EarningsDate_IU] '{}','{}' ".format(ticker, earningsdate)
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure update_next_earnings_date : {}".format(e))
    finally:
        cursor.close()
        conn.close()
def update_daily_quote_table(ticker, itemname, itemvalue, constr=str):
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        print("{}, {}, {}".format(ticker, itemname, itemvalue))
        proc = "exec dbo.[usp_Daily_Quote_Table_IU] '{}','{}','{}' ".format(ticker, itemname, itemvalue)
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure update_daily_quote_table : {}".format(e))
    finally:
        cursor.close()
        conn.close()
def update_daily_quote(ticker, itemname, itemvalue, constr=str):
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        print("{}, {}, {}".format(ticker, itemname, itemvalue))
        proc = "exec dbo.[usp_Daily_Quote_IU] '{}','{}','{}' ".format(ticker, itemname, itemvalue)
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure update_daily_quote : {}".format(e))
    finally:
        cursor.close()
        conn.close()
def update_stats_valuation(ticker=str, datequarter=str, itemname=str, itemvalue=str, constr=str):
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()

        proc = "exec dbo.[usp_Stats_Valuation_IU] '{}','{}','{}','{}' ".format(ticker, datequarter, itemname, itemvalue)
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()


def update_stats_category_info(category=str,itemname=str,constr=str):
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.usp_Stats_Category_IU '{}','{}'".format(category, itemname)
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()


def update_stats(ticker=str, yearQty=str, itemname=str, itemvalue=str, constr=str):
    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.usp_Stats_IU '{}','{}','{}', '{}'".format(ticker, yearQty, itemname, itemvalue)
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()

def update_company_info(ticker,sector,industry,website,constr=str):
    try:
        # stored proc with parameter
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.usp_Securitymaster_IU '{}','{}','{}','{}'".format(ticker, sector, industry,website)
        cursor.execute(proc)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()
def  update_analysts_info(ticker=str, dict_data = dict, constr=str):
    try:
        # stored proc with parameter
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.usp_Analyst_IU '{}','{}','{}','{}','{}'"

        for key, df_values in dict_data.items():
            for index, df_row in df_values.iterrows():
                df_row.fillna(0, inplace=True)

                for item, vlue in df_row[1:].items():
                    # print("Year={},Category = {}, item={} Values={}".format(item, df_row.index[0], df_row.iloc[0], vlue))
                    sp = proc.format(ticker,df_row.index[0],item, df_row.iloc[0], vlue)
                    cursor.execute(sp)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()
#
# def update_price_abu(ticker, df_price):
#     try:
#         conn = dbconnect.connect()
#         cursor = conn.cursor()
#         for index, row in df_price.iterrows():
#             parameters = [ticker, row['Unnamed: 0'], row.open, row.high, row.low, row.close, row.p_change,
#                           row.pre_close, row.volume, row.date, row.date_week, row.atr21,row.atr14]
#             cursor.callproc('usp_Price_Abu_IU', parameters)
#         conn.commit()
#     except Error as e:
#         print("Failed to execute stored procedure: {}".format(e))
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()
#         # print("MySQL connection is closed")
#
def update_price(df_price=pd.DataFrame, constr=str):
    # for index, row in df_price.iterrows():
    #     print("{0} {1},{2},{3}".format(row.ticker, row.date, row.close, row.close))

    try:
        # stored proc with parameter
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.usp_PriceAtr_IU '{}','{}',{},{},{},{},{},{}"
        for index, row in df_price.iterrows():
           ss=  proc.format(row.ticker, row.date, row.open, row.high, row.low, row.close, row.adjclose, row.volume)
                          # row.returns, row.atr21_ewm, row.atr21_ma, row.atr14_ewm, row.atr14_ma)
           print(ss)
           cursor.execute(ss)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()
        # print("MySQL connection is closed")
def update_price_atr(df_price=pd.DataFrame, constr=str):

    try:

        conn = pyodbc.connect(constr)
        cursor = conn.cursor()
        proc = "exec dbo.usp_PriceAtr_IU '{}','{}',{},{},{},{},{},{},{},{},{},{},{}"
        for index, row in df_price.iterrows():
           ss=  proc.format(row.ticker, row.date, round(row.open,2), round(row.high,2), round(row.low,2),
                            round(row.close,2), round(row.adjclose,2), round(row.volume,2),
                            round(row.atr21_ewm,2), round(row.atr21_ma,2), round(row.atr14_ewm,2),
                            round(row.atr14_ma,2),round(row.returns,4))

           cursor.execute(ss)
        conn.commit()
    except pyodbc.Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()

# def select_price(symbol,holding_period, start, end):
#     try:
#         conn = dbconnect.connect()
#         cursor = conn.cursor()
#         parameters=[symbol,holding_period,]
#         store_proc='usp_Price_Holding_Year_Sel'
#
#         # calling = 'call usp_Price_Sel("ABNB");'
#         # data = pd.read_sql(calling, conn, params=parameters)
#
#         cursor.callproc(store_proc,parameters)
#         # for colid in cursor.stored_results():
#         #     columnsProperties = (colid.)
#         #     print([column[0] for column in columnsProperties])
#
#         for result in cursor.stored_results():
#             column_names = result.column_names
#             data = result.fetchall()
#
#         # LOAD INTO A DATAFRAME
#         df = pd.DataFrame(data, columns=column_names)
#         df['key'] = list(range(0, len(df)))
#         df.symbol = symbol
#
#         return df
#
#     except Error as e:
#         print("Failed to execute stored procedure: {}".format(e))
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()
#         # print("MySQL connection is closed")
#
#
def update_balance_sheet(dict_balance_sheet,  constr=str):

    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()

        for ticker, df_sheet in dict_balance_sheet.items():
            df_sheet.fillna(0, inplace=True)
            dict_sheet = df_sheet.to_dict()
            for endDate, row in dict_sheet.items():
                # print( type(endDate)) # <class 'pandas._libs.tslibs.timestamps.Timestamp'>
                dt_endDate =  endDate.to_pydatetime()
                print( type(dt_endDate))

                for item, vlue in row.items():
                    parameters = [ticker, dt_endDate, item, vlue]
                    cursor.callproc('usp_BalanceSheet_IU', parameters)

        conn.commit()
    except pyodbc.Error  as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def update_income_statement(dict_income_statement , constr=None):

    try:

        conn = pyodbc.connect(constr)
        cursor = conn.cursor()

        for ticker, df_sheet in dict_income_statement.items():
            df_sheet.fillna(0, inplace=True)
            dict_sheet = df_sheet.to_dict()
            proc = "exec usp_IncomeStatement_IU '{}','{}','{}',{}"
            for endDate, row in dict_sheet.items():
                dt_endDate =  endDate.to_pydatetime()
                for item, vlue in row.items():
                    proc.format(ticker, dt_endDate, item, vlue)

        conn.commit()
    except pyodbc.Error  as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        cursor.close()
        conn.close()


def update_cash_flow(dict_cash_flow , constr=str):

    try:
        conn = pyodbc.connect(constr)
        cursor = conn.cursor()

        for ticker, df_sheet in dict_cash_flow.items():
            df_sheet.fillna(0, inplace=True)
            dict_sheet = df_sheet.to_dict()
            for endDate, row in dict_sheet.items():
                dt_endDate =  endDate.to_pydatetime()
                for item, vlue in row.items():
                    parameters = [ticker, dt_endDate, item, vlue]
                    cursor.callproc('usp_CashFlow_IU', parameters)

        conn.commit()
    except pyodbc.Error  as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()