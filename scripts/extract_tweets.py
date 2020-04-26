import oauth2
import json
import time
import os
import logging.config
import yaml
from config import twitter_config
from scripts import spark_postgres_ETL

# logging configuration
logging_config = os.path.join(os.pardir, "config", 'logging.yaml')

# filename to store the tweets
fname = os.path.join(os.pardir, "data", twitter_config.track + '.json')

data = {}

# logging setup
def setup_logging(logging_config, default_level=logging.INFO):
    if os.path.exists(logging_config):
        with open(logging_config, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


# OAuth authentication for Twitter
def oauth_req(url, http_method="GET", post_body=b"", http_headers=None):
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


# Search twitter tweets
def get_tweets():
    global keyword, start_date, end_date, lang
    return oauth_req(
        'https://api.twitter.com/1.1/search/tweets.json?&q=' + twitter_config.track + '&lang=' + twitter_config.lang + '&result_type=mixed&count=100')


if __name__ == '__main__':

    setup_logging(logging_config)
    logger = logging.getLogger(__name__)
    # Store collected tweets to tweets.json
    try:
        data = json.loads(get_tweets())
        with open(fname, 'w+') as f:
            json.dump(data, f)
        time.sleep(5)
        logger.info("Tweets Collected!")
    except:
        logger.error("Error while fetching Tweets!")

    # Triggering Spark-Postgres ETL Job
    try:
        logger.info("Triggering Spark-Postgres ETL Job.")
        spark_postgres_ETL.main(fname)
    except:
        logger.error("Error while triggering Spark Job")
