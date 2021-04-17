from datetime import datetime
import os
import pickle
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType, StringType
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql import functions as sf


class PostgresClient:
    """Class for interacting with postgres database
    """


    def __init__(self):
        """Initiate the variables
        """
        self.spark = None

        # Postgres configurations
        self.host =os.getenv("POSTGRES_HOST", "localhost")
        self.port =os.getenv("POSTGRES_PORT", 5432)
        self.user = os.getenv("POSTGRES_USER", "update_me")
        self.password = os.getenv("POSTGRES_PASSWORD", "update_me")
        self.database = "ge_db"
        self.driver = "org.postgresql.Driver"
        self.url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

        
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

        
    def read_from_POSTGRES_spark(self, dbtable):
        """Read data from Postgres in spark dataframe

        Parameters
        ----------
        dbtable : str
            query or table name

        Returns
        -------
        pandas.core.frame.DataFrame
            Return pandas dataframe
        """
        spark = self.spark
        df = spark.read\
            .format("jdbc")\
            .option("url", self.url)\
            .option("driver", self.driver)\
            .option("dbtable", dbtable)\
            .option("user",self.user)\
            .option("password",self.password)\
            .option("inferSchema",False) \
            .load()
        return df


if __name__ == '__main__':

    # Define spark app name
    appname = "Postgres Helper"

    # Create an object of SQLClient
    postgres_client = PostgresClient()

    # Create spark session:
    postgres_client.create_spark_session()

    # Read data from POSTGRES
    dbtable = "(select * from products limit 1000) as emp" 
    df_postgres = postgres_client.read_from_POSTGRES_spark(dbtable)
    df_postgres.show(5)

    # Stop the spark session
    postgres_client.stop_spark_session()
