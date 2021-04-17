from datetime import datetime
import os
import pickle
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType, StringType
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql import functions as sf


class SQLClient:
    def __init__(self):
        """Initiate the variables
        """
        self.spark = None

        # MySQL configurations
        self.host =os.getenv("MYSQL_HOST", "localhost")
        self.port =os.getenv("MYSQL_PORT", 3306)
        self.user = os.getenv("MYSQL_USER", "update_me")
        self.password = os.getenv("MYSQL_PASSWORD", "update_me")
        self.driver = "com.mysql.jdbc.Driver"
        self.url = f"jdbc:mysql://{self.host}:{self.port}/"

        
    def create_spark_session(self):
        """Function to create a spark session
        """

        spark_jar_path = os.getenv("SPARK_JARS_PATH")
        spark_jars = [os.path.join(spark_jar_path, jars) for jars in os.listdir(spark_jar_path)]      

        self.spark = SparkSession\
            .builder\
            .config("spark.jars", ",".join(spark_jars))\
            .appName(appname)\
            .getOrCreate()


    def stop_spark_session(self):
        """Function to stop the spark session
        """
        self.spark.stop()

        
    def read_from_mysql_spark(self, dbtable):
        """Read data from mysql in spark dataframe

        Parameters
        ----------
        dbtable : str
            sql query or table name

        Returns
        -------
        pandas.core.frame.DataFrame
            Return pandas dataframe
        """
        spark = self.spark
        df = spark.read.format("jdbc")\
                .option("url", self.url)\
                .option("driver", self.driver)\
                .option("dbtable", dbtable)\
                .option("user",self.user)\
                .option("password",self.password)\
                .load()
        return df


if __name__ == '__main__':

    # Define spark app name
    appname = "MYSQL Helper"

    # Create an object of SQLClient
    sql_client = SQLClient()

    # Create spark session:
    sql_client.create_spark_session()

    # Read data from MYSQL
    dbtable = "(select * from classicmodels.customers) as emp"
    df_sql = sql_client.read_from_mysql_spark(dbtable)
    df_sql.show(5)

    # Stop the spark session
    sql_client.stop_spark_session()
