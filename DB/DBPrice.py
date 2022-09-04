# from datetime import datetime
# import pandas as pd
# import mysql.connector

import pandas as pd
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
# def update_price(df_price):
#     try:
#         conn = dbconnect.connect()
#         cursor = conn.cursor()
#         for index, row in df_price.iterrows():
#             parameters = [row.ticker, row.date, row.open, row.high, row.low, row.close, row.adjclose,
#                           row.returns, row.volume, row.atr21_ewm, row.atr21_ma, row.atr14_ewm, row.atr14_ma]
#             cursor.callproc('usp_Price_IU', parameters)
#         conn.commit()
#     except Error as e:
#         print("Failed to execute stored procedure: {}".format(e))
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()
#         # print("MySQL connection is closed")
#
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
# def update_balance_sheet(dict_balance_sheet):
#
#     try:
#         conn = dbconnect.connect()
#         cursor = conn.cursor()
#
#         for ticker, df_sheet in dict_balance_sheet.items():
#             df_sheet.fillna(0, inplace=True)
#             dict_sheet = df_sheet.to_dict()
#             for endDate, row in dict_sheet.items():
#                 # print( type(endDate)) # <class 'pandas._libs.tslibs.timestamps.Timestamp'>
#                 dt_endDate =  endDate.to_pydatetime()
#                 print( type(dt_endDate))
#
#                 for item, vlue in row.items():
#                     parameters = [ticker, dt_endDate, item, vlue]
#                     cursor.callproc('usp_BalanceSheet_IU', parameters)
#
#         conn.commit()
#     except Error as e:
#         print("Failed to execute stored procedure: {}".format(e))
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()
#
#
# def update_income_statement(dict_income_statement):
#
#     try:
#         conn = dbconnect.connect()
#         cursor = conn.cursor()
#
#         for ticker, df_sheet in dict_income_statement.items():
#             df_sheet.fillna(0, inplace=True)
#             dict_sheet = df_sheet.to_dict()
#             for endDate, row in dict_sheet.items():
#                 dt_endDate =  endDate.to_pydatetime()
#                 for item, vlue in row.items():
#                     parameters = [ticker, dt_endDate, item, vlue]
#                     cursor.callproc('usp_IncomeStatement_IU', parameters)
#
#         conn.commit()
#     except Error as e:
#         print("Failed to execute stored procedure: {}".format(e))
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()
#
#
# def update_cash_flow(dict_cash_flow):
#
#     try:
#         conn = dbconnect.connect()
#         cursor = conn.cursor()
#
#         for ticker, df_sheet in dict_cash_flow.items():
#             df_sheet.fillna(0, inplace=True)
#             dict_sheet = df_sheet.to_dict()
#             for endDate, row in dict_sheet.items():
#                 dt_endDate =  endDate.to_pydatetime()
#                 for item, vlue in row.items():
#                     parameters = [ticker, dt_endDate, item, vlue]
#                     cursor.callproc('usp_CashFlow_IU', parameters)
#
#         conn.commit()
#     except Error as e:
#         print("Failed to execute stored procedure: {}".format(e))
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()