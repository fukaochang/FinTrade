import logging
import os
import re
import platform
import sys
import warnings
from enum import Enum
from os import path
import numpy as np
import pandas as pd
import six
import psutil
from configparser import ConfigParser
from Util import FileUtil


"""主进程pid，使用并行时由于ABuEnvProcess会拷贝主进程注册了的模块信息，所以可以用g_main_pid来判断是否在主进程"""
g_main_pid = os.getpid()

"""有psutil，使用psutil.cpu_count计算cpu个数"""
g_cpu_cnt = psutil.cpu_count(logical=True) * 1

# ＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊ 数据目录 start ＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
"""
    文件目录根目录
    windows应该使用磁盘空间比较充足的盘符，比如：d://, e:/, f:///

    eg:
    root_drive = 'd://'
    root_drive = 'e://'
    root_drive = 'f://'
"""
root_drive = path.expanduser('~')

"""数据缓存主目录文件夹"""
g_project_root = path.join(root_drive, 'FinTrade')
"""数据文件夹 ~/FinTrade/data"""
g_project_data_dir = path.join(g_project_root, 'sourcefile')
"""日志文件夹 ~/FinTrade/log"""
g_project_log_dir = path.join(g_project_root, 'log')
"""数据库文件夹 ~/FinTrade/db"""
g_project_db_dir = path.join(g_project_root, 'db')
"""缓存文件夹 ~/FinTrade/cache"""



"""市场中1年交易日，默认250日"""
# 美股252天
g_market_trade_year = 252


g_mysql_connection = {}
g_mssql_connection = {}
g_price_file = {}
g_tick_list = {}
g_benchmark={}
g_globaldb_constr=""

class ConfigSection(Enum):
    E_MYSQL = 'mysql'
    E_MSSQL='mssql'
    E_PRICE_FILE = 'pricefile'
    E_TICKER = 'ticker'
    E_BENCHMARK ='benchmark'

def _listfstr( _dict ):
    """
     no empty strings at the start or end of the string if the string has leading or trailing whitespace.
    :param _dict:
    :return: _dict
    """

    for k, v in _dict.items():
        _dict[k] = v.split()
        # if ',' in v:
            # _dict[k] = "".join(v.split()).split(',')
    return  _dict

def read_config(filename='config.ini'):

    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    global g_mysql_connection
    global g_mssql_connection
    global g_price_file
    global g_tick_list
    global g_benchmark
    global g_globaldb_constr

    def configSectionMap(p_section):
        dict1 = {}
        options = parser.options(p_section)
        for option in options:
            try:
                dict1[option] = parser.get(p_section, option)
                if dict1[option] == -1: 
                    print("skip: %s" % option)
            except Exception as ex:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1

    if not FileUtil.file_exist(filename):
        print("configuration file {} does not exit.".format(filename))
        return

    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)
    sections = parser.sections()
    for section in parser.sections():

        if section == ConfigSection.E_MYSQL.value:
            g_mysql_connection = configSectionMap(section)
        elif section == ConfigSection.E_MSSQL.value:
            g_mssql_connection = configSectionMap(section)
        elif section == ConfigSection.E_PRICE_FILE.value:
            g_price_file = configSectionMap(section)
        elif section == ConfigSection.E_TICKER.value:
            g_tick_list = _listfstr ( configSectionMap(section) )
        elif section == ConfigSection.E_BENCHMARK.value:
            g_benchmark = configSectionMap(section)

    if g_mssql_connection:
        g_globaldb_constr = g_mssql_connection.get('connectionstring')

"""
 # get section, default to mysql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    # {'host': 'localhost', 'database': 'quant', 'user': 'jchang', 'password': 'shulin0803'}
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))
"""


# Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     read_config('../config.ini')
#     print(g_mysql_connection)
#     print(g_price_file)