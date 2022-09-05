import functools
from concurrent.futures import ProcessPoolExecutor
from Util import SystemEnv

class Parallel(object):
    """封装ProcessPoolExecutor进行并行任务执行操作"""

    def __init__(self, n_jobs=1, backend='multiprocessing', verbose=0,
                 pre_dispatch='2 * n_jobs', batch_size='auto',
                 temp_folder=None, max_nbytes='1M', mmap_mode='r'):
        """
        :param n_jobs: 并行启动的进程数，任务数量
        :param backend: 无意义，只是为了统一接口规范，与joblib.Parallel保持一样的参数
        :param verbose: 无意义，只是为了统一接口规范，与joblib.Parallel保持一样的参数
        :param pre_dispatch: 无意义，只是为了统一接口规范，与joblib.Parallel保持一样的参数
        :param batch_size: 无意义，只是为了统一接口规范，与joblib.Parallel保持一样的参数
        :param temp_folder: 无意义，只是为了统一接口规范，与joblib.Parallel保持一样的参数
        :param max_nbytes: 无意义，只是为了统一接口规范，与joblib.Parallel保持一样的参数
        :param mmap_mode: 无意义，只是为了统一接口规范，与joblib.Parallel保持一样的参数
        """
        self.n_jobs = n_jobs

    def __call__(self, iterable):
        """为与joblib并行保持一致，内部使用ProcessPoolExecutor开始工作"""

        result = []

        def when_done(r):
            """ProcessPoolExecutor每一个进程结束后结果append到result中"""
            result.append(r.result())

        if self.n_jobs <= 0:
            # 主要为了适配 n_jobs = -1，joblib中启动cpu个数个进程并行执行
            self.n_jobs = SystemEnv.g_cpu_cnt

        if self.n_jobs == 1:
            # 如果只开一个进程，那么只在主进程(或当前运行的子进程)里运行，方便pdb debug且与joblib运行方式保持一致
            for jb in iterable:
                result.append(jb[0](*jb[1], **jb[2]))
        else:
            with ProcessPoolExecutor(max_workers=self.n_jobs) as pool:
                for jb in iterable:
                    # 这里iterable里每一个元素是delayed.delayed_function保留的tuple
                    future_result = pool.submit(jb[0], *jb[1], **jb[2])
                    future_result.add_done_callback(when_done)
        return result