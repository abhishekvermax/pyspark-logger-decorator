
import unittest
from src.main.spark_job import create_df, df_analysis
from test.samplesparksession import SampleSparkSession
from pyspark.sql import DataFrame
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SampleSparkSession().new_spark_session()


class Test_Spark_Job_Logging(unittest.TestCase):      

    
    def test_create_df(self):
        return create_df(ss=spark)
    
    def test_log_df_details(self):
        self.assertEqual(df_analysis(create_df(ss=spark)),5)

if __name__ == "__main__":
    unittest.main()
