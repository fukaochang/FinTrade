import datetime
from functools import wraps

def logit(logfile='.\logfile.txt'):
    def logging_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            log_string = func.__name__ + " was called"
            print(log_string)
            # 打开logfile，并写入内容
            with open(logfile, 'a') as opened_file:
                # 现在将日志打到指定的logfile
                opened_file.write(log_string + '\n')
            return func(*args, **kwargs)
        return wrapped_function
    return logging_decorator

def elapse_time(calling_function):
    def wrapper(*args, **kwargs):
        before = datetime.datetime.now()
        x = calling_function(*args, **kwargs)
        after = datetime.datetime.now()
        elapsetime = after - before
        print("Elapsed Time = {0} seconds".format(elapsetime.total_seconds()))
        return x
    return wrapper
