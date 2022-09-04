import logging
import os
import pickle
import shutil
import six


import pandas as pd

"""HDF5_COMP_LEVEL：压缩级别：0-9，如果修改了压缩级别，需要删除之前的物理文件"""
HDF5_COMP_LEVEL = 4
""" HDF5_COMP_LIB: 使用的压缩库:'blosc', 'bzip2', 'lzo', 'zlib', 如果修改了压缩库，需要删除之前的物理文件"""
HDF5_COMP_LIB = 'blosc'

"""HDF5内部存贮依然会使用pickle，即python版本切换，本地文件会有协议冲突：使用所支持的最高协议进行dump"""
K_SET_PICKLE_HIGHEST_PROTOCOL = False
"""HDF5内部存贮依然会使用pickle，即python版本切换，本地文件会有协议冲突：python2, python3协议兼容模式，使用protocol=0"""
K_SET_PICKLE_ZERO_PROTOCOL = False


def ensure_dir(a_path):
    """
    确保a_path所在路径文件夹存在，如果a_path是文件将确保它上一级
    文件夹的存在
    :param a_path: str对象, 相对路径或者绝对路径
    """
    if os.path.isdir(a_path):
        a_dir = a_path
    else:
        a_dir = os.path.dirname(a_path)
    if not os.path.exists(a_dir):
        os.makedirs(a_dir)


def ensure_file(a_path):
    """
    确保a_path所在路径文件存在，首先会使用ensure_dir确保文件夹存在，
    确定a_path是file会确保文件存在
    :param a_path: str对象, 相对路径或者绝对路径
    :return:
    """
    ensure_dir(a_path)
    open(a_path, 'a+').close()


def file_exist(a_path):
    """
    a_path是否存在
    :param a_path: str对象, 相对路径或者绝对路径
    """
    return os.path.exists(a_path)


def copy_file(source, target_dir):
    """
    拷贝文件操作，支持文件夹拷贝操作
    :param source: 文件名或者文件夹，str对象, 相对路径或者绝对路径
    :param target_dir: 文件名或者文件夹，str对象, 相对路径或者绝对路径
    """

    if os.path.exists(source):
        logging.error('copy_file source={} not exists!'.format(source))
        return

    ensure_dir(target_dir)

    if os.path.isdir(source):
        shutil.copytree(source, target_dir)
    else:
        shutil.copy(source, target_dir)


def del_file(a_path):
    """
    删除文件操作，支持文件夹删除操作
    :param a_path: 文件名或者文件夹，str对象, 相对路径或者绝对路径
    """
    if not file_exist(a_path):
        return

    if os.path.isdir(a_path):
        shutil.rmtree(a_path)
    else:
        os.remove(a_path)

        def load_pickle(file_name):
            """
            读取python序列化的本地文件
            :param file_name: 文件名，str对象, 相对路径或者绝对路径
            :return:
            """
            if not file_exist(file_name):
                logging.error('load_pickle file_name={} not exists!'.format(file_name))
                return None

            # TODO 根据文件大小，决定这里是否需要tip wait
            print('please wait! load_pickle....:', file_name)

            try:
                with open(file_name, "rb") as unpickler_file:
                    unpickler = pickle.Unpickler(unpickler_file)
                    ret = unpickler.load()
            except EOFError:
                print('unpickler file with EOFError, please check {} is 0kb!!!'.format(file_name))
                ret = {}

            return ret

        def dump_pickle(input_obj, file_name, how='normal'):
            """
            存贮python序列化的本地文件
            :param input_obj: 需要进行序列化的对象
            :param file_name: 文件名，str对象, 相对路径或者绝对路径
            :param how: 序列化协议选择，默认normal不特殊处理，
                        zero使用python2, python3协议兼容模式，使用protocol=0，
                        high使用支持的最高协议
            """
            ensure_dir(file_name)

            print('please wait! dump_pickle....:', file_name)

            try:
                with open(file_name, "wb") as pick_file:
                    if K_SET_PICKLE_HIGHEST_PROTOCOL or how == 'high':
                        """使用所支持的最高协议进行dump"""
                        pickle.dump(input_obj, pick_file, pickle.HIGHEST_PROTOCOL)
                    elif K_SET_PICKLE_ZERO_PROTOCOL or how == 'zero':
                        """python2, python3协议兼容模式，使用protocol=0"""
                        pickle.dump(input_obj, pick_file, 0)
                    else:
                        pickler = pickle.Pickler(pick_file)
                        pickler.dump(input_obj)
            except Exception as e:
                logging.exception(e)


def load_df_csv(file_name):
    """
    从csv文件中实例化pd.DataFrame对象
    :param file_name: 保存csv的文件名称
    :return: pd.DataFrame对象
    """
    if file_exist(file_name):
        return pd.read_csv(file_name, index_col=0)
    return None


def save_file(ct, file_name):
    """
    将内容ct保存文件
    :param ct: 内容str对象
    :param file_name: 保存的文件名称
    :return:
    """
    ensure_dir(file_name)
    with open(file_name, 'wb') as f:
        f.write(ct)
