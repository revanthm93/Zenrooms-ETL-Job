from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
import json, yaml, oauth2, time, os
import logging, logging.config, configparser
from config import twitter_config

logger = logging.getLogger(__name__)
data = {}
config_path = os.path.join(os.pardir, "config", 'db_properties.ini')
logging_config = os.path.join(os.pardir, "config", 'logging.yaml')
fname = os.path.join(os.pardir, "data", twitter_config.track + '.json')
jars = os.path.join(os.pardir, "jars", 'postgresql-42.2.12.jar')


def main():
    setup_logging(logging_config)
    extract_data(fname)


def get_dbprop1(config_path):
    # parsing db properties from config file
    db_properties = {}
    dbconfig = configparser.ConfigParser()
    dbconfig.read(config_path)
    db_prop = dbconfig['postgresql']
    db_url = db_prop['url']
    db_properties['user'] = db_prop['user']
    db_properties['password'] = db_prop['password']
    db_properties['driver'] = db_prop['driver']
    return db_url, db_properties


def setup_logging(logging_config, default_level=logging.INFO):
    # logging setup
    if os.path.exists(logging_config):
        with open(logging_config, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def oauth_req(url, http_method="GET", post_body=b"", http_headers=None):
    # OAuth authentication for Twitter
    try:
        logger.info("Authenticating to Twitter API")
        consumer = oauth2.Consumer(key=twitter_config.consumerKey, secret=twitter_config.consumerSecret)
        token = oauth2.Token(key=twitter_config.accessToken, secret=twitter_config.accessTokenSecret)
        client = oauth2.Client(consumer, token)
        resp, content = client.request(url, method=http_method, body=post_body, headers=http_headers)
        logger.info("OAuth authentication successful.")
        return content
    except Exception as e:
        logger.error(e)


def get_tweets():
    # Search twitter tweets
    return oauth_req(
        'https://api.twitter.com/1.1/search/tweets.json?&q=' + twitter_config.track + '&lang=' + twitter_config.lang + '&result_type=mixed&count=100')


def extract_data(fname):
    # Extracts data from twiiter API and writes to json file.
    try:
        data = json.loads(get_tweets())
        with open(fname, 'w+') as f:
            json.dump(data, f)
        time.sleep(5)
        logger.info("Tweets Collected!")
        transform_data(fname)
    except Exception as e:
        logger.error("Error while fetching Tweets! %s", e)


def transform_data(fname):
    # transform, clean and filter dataframe
    try:
        logger.info("Creating Spark Session")
        spark = SparkSession.builder.master('local[*]').appName('zenrooms_etljob').config("spark.jars",
                                                                                          jars).getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        raw_data = spark.read.json(fname)
        logger.info("Cleaning data by dropping unnecessary fields")
        df = raw_data.drop("search_metadata").withColumn("statuses", explode("statuses")).select("statuses.*") \
            .withColumnRenamed("id", "tweetid").select("tweetid", "text", "user.*").drop("entities", "description")
        logger.info("Filtering data by location (India)")
        filtered_df = df.filter(col("location").contains("India"))
        cols = [c for c in filtered_df.columns if c.lower()[:7] != 'profile']
        logger.info("Writing processed data to postgres")
        load_data(spark, filtered_df[cols])
    except Exception as e:
        logger.error("Error in processing data, probably corrupt/malformed file. %s", e)


def load_data(spark, df):
    # load the dataframe to the postgres database table.
    try:
        db_url, db_properties = get_dbprop1(config_path)
        df.write.mode("append").jdbc(url=db_url, table='tweets', properties=db_properties)
        logger.info("Processed data successfully loaded to Postgres table : tweets!")
        # spark.stop()
    except Exception as e:
        logger.error("Error while writing data to Postgres Table. %s ", e)


if __name__ == '__main__':
    main()
