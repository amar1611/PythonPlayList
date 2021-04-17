import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType, StringType
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql import functions as sf


class ElasticsearchClient:
    def __init__(self):
        """Initiate the variables
        """
        self.spark = None

        # Elasticsearch configurations
        self.es_host =os.getenv("ELASTICSEARCH_HOST", "localhost")
        self.es_port =os.getenv("ELASTICSEARCH_PORT", 9200)
        self.es_url = f"http://{self.es_host}:{self.es_port}"
        self.es_user = os.getenv("ELASTICSEARCH_USER", "update_me")
        self.es_password = os.getenv("ELASTICSEARCH_PASSWORD", "update_me")
        self.es_index_name = f"es_index_{str(datetime.datetime.now().date()).replace('-','_')}"
        print(f"Index Name: {self.es_index_name}")

        
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


    def get_sample_spak_dataframe(self):
        """Create a sample spark dataframe

        Returns
        -------
        pyspark.sql.dataframe.DataFrame
            Sample spark dataframe
        """
        sample_data = [
            ("Amar", 30),
            ("Archana", 25),
            ("Amrita", 18),
            ("Rajnish", 17),
            ("Dummy", None)
        ]
        colums = ("Name", "Age")
        df = self.spark.createDataFrame(sample_data, colums)
        return df


    def dataCleaning(self, df):
        """Clean the dataframe before inserting it to elasticsearch

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Dataframe having null values

        Returns
        -------
        pyspark.sql.dataframe.DataFrame
            Dataframe after removing null values and typecasting datatypes supportd by elasticsearch
        """
        if df.count() != 0:
            for column, dtype in df.dtypes:
                if ("date" in str(dtype)) or ("time" in str(dtype)):
                    df = df.withColumn(column, sf.date_format(df[column], "yyyy-MM-dd'T'HH:mm:ss.SSS"))
                    df = df.withColumn(column, df[column].cast("string"))
                if ("int" in str(dtype)) or ("double" in str(dtype)) or ("float" in str(dtype)):
                    df = df.fillna(0, column)
                if "string" in str(dtype):
                    df = df.fillna("nan", column)
                if "boolean" in str(dtype):
                    df = df.fillna("nan", column)
                if "decimal" in str(dtype):
                    df = df.withColumn(column, df[column].cast("float"))
                if "array" in str(dtype):
                    df = df.withColumn(column, df[column].cast("string"))
        return df


    def write_data_to_elasticsearch_spark(self, df):
        """Insert data into elasticsearch index

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Dataframe for writing into elasticsearch
        """
        try:
            df.write\
                .format("org.elasticsearch.spark.sql")\
                .option("es.nodes", self.es_url)\
                .option("es.net.http.auth.user",self.es_user)\
                .option("es.net.http.auth.pass",self.es_password)\
                .mode("append")\
                .save(self.es_index_name)
        except Exception as exp:
            print(str(exp))    


if __name__ == '__main__':

    # Define spark app name
    appname = "Elasticsearch Helper"

    # Create an object of ElasticsearchClient
    es_client = ElasticsearchClient()

    # Create spark session:
    es_client.create_spark_session()

    # Read sample data from csv file
    df = es_client.get_sample_spak_dataframe()
    df.show(5)

    # Clean the data before inserting
    df_cleaned = es_client.dataCleaning(df)
    df_cleaned.show(5)

    # Write the data to Elasticsearch
    es_client.write_data_to_elasticsearch_spark(df_cleaned)
    
    # Stop the spark session
    es_client.stop_spark_session()
