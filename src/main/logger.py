import logging
import logging.config
import os
from pathlib import Path
import tempfile
from logging import *  # gives access to logging.DEBUG etc by aliasing this module for the standard logging module
import urllib.request
import json
from datetime import datetime
from pyspark.sql import SparkSession,DataFrame
import inspect
from functools import wraps


LOGS_DIR = str(Path(__file__).parent.parent) + '/logs/'

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)
    
os.environ['LOG_DIRS'] = LOGS_DIR


class Unique(logging.Filter):
    """Messages are allowed through just once.
    The 'message' includes substitutions, but is not formatted by the
    handler. If it were, then practically all messages would be unique!
    """
    def __init__(self, name=""):
        logging.Filter.__init__(self, name)
        self.reset()

    def reset(self):
        """Act as if nothing has happened."""
        self.__logged = {}

    def filter(self, rec):
        """logging.Filter.filter performs an extra filter on the name."""
        return logging.Filter.filter(self, rec) and self.__is_first_time(rec)

    def __is_first_time(self, rec):
        """Emit a message only once."""
        msg = rec.msg %(rec.args)
        if msg in self.__logged:
            self.__logged[msg] += 1
            return False
        else:
            self.__logged[msg] = 1
            return True


def getLogger(name, logfile="pyspark.log"):
    """Replaces getLogger from logging to ensure each worker configures
    logging locally."""

    try:
        logfile = os.path.join(os.environ['LOG_DIRS'].split(',')[0], logfile)
    except (KeyError, IndexError):
        tmpdir = tempfile.gettempdir()
        logfile = os.path.join(tmpdir, logfile)
        rootlogger = logging.getLogger("")
        rootlogger.addFilter(Unique())
        rootlogger.warning(
            "LOG_DIRS not in environment variables or is empty. Will log to {}."
            .format(logfile))

    # Alternatively, load log settings from YAML or use JSON.
    log_settings = {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
                'filename': logfile
            },
            'default': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
            },
        },
        'formatters': {
            'detailed': {
                'format': ("%(asctime)s.%(msecs)03d %(levelname)s %(module)s - "
                           "%(funcName)s: %(message)s"),
            },
        },
        'loggers': {
            'driver': {
                'level': 'INFO',
                'handlers': ['file', ]
            },
            'executor': {
                'level': 'DEBUG',
                'handlers': ['file', ]
            },
        }
    }

    logging.config.dictConfig(log_settings)
    return logging.getLogger(name)

APP_NAME = ""
APP_ID = ""

def get_spark_job_id(spark:SparkSession):
    global APP_ID
    sc = spark.sparkContext
    APP_ID = sc.applicationId
    print(APP_ID)
    return APP_ID

def get_spark_job_name(spark:SparkSession):
    global APP_NAME
    sc = spark.sparkContext
    APP_NAME = sc.appName
    print(APP_NAME)
    return APP_NAME

def get_executor_details(spark:SparkSession):
    details = [[]]
    sc = spark.sparkContext
    u = sc.uiWebUrl + '/api/v1/applications/' + sc.applicationId + '/allexecutors'
    with urllib.request.urlopen(u) as url:
        executors_data = json.loads(url.read().decode())
        details.append(executors_data)
    return "\n".join(["\n".join([str(z) for z in x]) for x in details])


def get_df_details(df:DataFrame):
    df_details = df.rdd.toDebugString().decode()
    return df_details

def get_df_schema(df:DataFrame):
    df_schema = df.schema.json()
    return df_schema

def get_log_file_name():
    log_file_name = APP_NAME+"_"+APP_ID+"_"+"spark.log"
    return log_file_name

def log(msg:str):
    log = getLogger("driver",get_log_file_name())
    log.info(msg)

def get_df_logged(df, n=20, truncate=True, vertical=False,limit=5):
    if isinstance(truncate, bool) and truncate:
        return(df.limit(limit)._jdf.showString(n, 20, vertical))
    else:
        return(df.limit(limit)._jdf.showString(n, int(truncate), vertical))


def log_df(func):
    """
    Decorator to log dataframe details.

    This includes parameters names and effective values.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        df_arg = [arg[1] for arg in func_args.items() if isinstance(arg[1],DataFrame)][0]
        func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
        log(msg=f'Function: <{func.__qualname__},({func_args_str}) > Run on: {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')
        log(msg="\n"+get_df_logged(df_arg))
        return func(*args, **kwargs)
    return wrapper


def log_df_schema(func):
    """
    Decorator to log dataframe details.

    This includes parameters names and effective values.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        df_arg = [arg[1] for arg in func_args.items() if isinstance(arg[1],DataFrame)][0]
        func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
        log(msg=f'Function: <{func.__qualname__},({func_args_str}) > Run on: {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')
        log(msg="\n"+get_df_schema(df_arg))
        return func(*args, **kwargs)
    return wrapper



def log_df_details(func):
    """
    Decorator to log dataframe details.

    This includes parameters names and effective values.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        df_arg = [arg[1] for arg in func_args.items() if isinstance(arg[1],DataFrame)][0]
        func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
        log(msg=f'Function: <{func.__qualname__},({func_args_str}) > Run on: {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')
        log(msg="\n"+get_df_details(df_arg))
        return func(*args, **kwargs)
    return wrapper



def log_spark_job_details(func):
    """
    Decorator to Spark job details.

    This includes parameters names and effective values.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        spark_session_arg = [arg[1] for arg in func_args.items() if isinstance(arg[1],SparkSession)][0]
        func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
        log(msg=f'Function: <{func.__qualname__},({func_args_str}) > Run on: {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')
        log(msg="SPARK JOB Name : "+ str(get_spark_job_name(spark_session_arg)).replace(" ","_")+"_"+get_spark_job_id(spark_session_arg))
        log(msg="Below are the executor details : "+"\n"+get_executor_details(spark=spark_session_arg))
        return func(*args, **kwargs)
    return wrapper


def log_function_details(func):
    """
    Decorator to print function call details.

    This includes parameters names and effective values.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
        log(msg=f'Function: <{func.__qualname__},({func_args_str}) > Run on: {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')
        return func(*args, **kwargs)
    return wrapper