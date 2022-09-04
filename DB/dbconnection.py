import pandas as pd
import pyodbc
import pandas.io.sql as psql
import sqlalchemy as sa
import urllib
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
import pymssql

from Util import  SystemEnv


def f_sqlalchemy(qry,constr) -> pd.DataFrame:
    params = urllib.parse.quote_plus(constr)
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    df = pd.read_sql_query(qry, con=engine)

    return df

#   ---------------------------------------------------------
#   Using pyodbc
#   ---------------------------------------------------------
def f_pyodbc(qry,constr):
    # constr = SystemEnv.g_mssql_connection['connectionstring']
    conn = pyodbc.connect(constr)
    cursor = conn.cursor()
    cursor.execute(qry)
    row = cursor.fetchone()
    while row:
        print(row)
        row = cursor.fetchone()

def f_pyodbc_Sel(qry=str,constr=str) -> pd.DataFrame:

    conn = pyodbc.connect(constr)
    df = pd.read_sql(qry, conn)

    return df

def f_pyodbc_IU(qry, constr):
    # constr = SystemEnv.g_mssql_connection['connectionstring']
    conn = pyodbc.connect(constr)
    cursor = conn.cursor()
    cursor.execute(qry)
    # cursor.execute("INSERT INTO HumanResources.DepartmentTest (DepartmentID,Name,GroupName) values(?,?,?)",
    #                row.DepartmentID, row.Name, row.GroupName)
    conn.commit()
    cursor.close()