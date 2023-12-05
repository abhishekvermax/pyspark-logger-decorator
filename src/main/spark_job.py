from pyspark.sql import Row,SparkSession,DataFrame
from pyspark.sql.types import IntegerType

from src.main.logger import  log_df, log_df_details, log_df_schema, log_function_details, log_spark_job_details


@log_function_details
@log_spark_job_details
def create_df(ss:SparkSession):
    df = ss.createDataFrame([1,2,3,4,5], IntegerType())
    return df


@log_df_details
@log_df_schema
@log_df
def df_analysis(df:DataFrame):
    return df.count()