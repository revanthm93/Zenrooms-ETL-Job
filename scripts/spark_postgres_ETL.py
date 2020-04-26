from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from config import twitter_config
import configparser
import logging
import os
from scripts import extract_tweets

logging_config = os.path.join(os.pardir, "config", 'logging.yaml')

extract_tweets.setup_logging(logging_config)

logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("ZenRooms ETL Job") \
    .config("spark.jars", "../jars/postgresql-42.2.12.jar").getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


db_properties = {}
dbconfig = configparser.ConfigParser()
dbconfig.read(os.pardir + "/config/db_properties.ini")
db_prop = dbconfig['postgresql']
db_url = db_prop['url']
db_properties['user'] = db_prop['user']
db_properties['password'] = db_prop['password']
db_properties['driver'] = db_prop['driver']




def transform_and_filter(raw_data):
    try:
        logger.info("Cleaning data by dropping unnecessary fields")
        df = raw_data.drop("search_metadata").withColumn("statuses", explode("statuses")).select("statuses.*") \
            .withColumnRenamed("id", "tweetid").select("tweetid", "text", "user.*").drop("entities", "description")
        logger.info("Filtering data by location (India)")
        filtered_df = df.filter(col("location").contains("India"))
        cols = [c for c in filtered_df.columns if c.lower()[:7] != 'profile']
        logger.info("Writing processed data to postgres")
        return filtered_df[cols]
    except:
        logger.error("Error in processing data, probably corrupt/malformed file")


def main(fname):
    try:
        logger.info("Reading extracted json as Spark dataframe")
        raw_data = spark.read.json(fname)
    except Exception as e:
        logger.error(e)

    # Save the dataframe to the postgres database table.
    try:
        transform_and_filter(raw_data).write.mode("append").jdbc(url=db_url, table='tweets', properties=db_properties)
        spark.stop()
        logger.info("Processed data successfully loaded to Postgres table : tweets!")
    except Exception as e:
        logger.error("Error while writing data to Postgres Table. %s ",e)
