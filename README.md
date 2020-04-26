This project was developed for performing ETL on public data, given the requirements below:

Extract - FROM public API using OAuth authentication. (Twitter API + Python)
Transform - Transform and process data using open source framework. (Apache Spark)
Load - Store the processed data to database. (Postgres)

How the ETL solution works?

A secured OAuth authenticated connection will be established to Twitter API (extract_tweets.py), query tweets which contains the tracking-keyword (configurable through config/config.py), dump the result to a json file (data/<keyword>.json), calls the spark job (spark_postgres_ETL.py) bu itself which cleans and transform the data and write the processed data to postgres database table.

What is What?

requirements.txt - It consists of all the required python modules for this project. Simply use pip install -r requirements.txt

config:
logging.yaml - contains logging configurations.
twitter_config.py - contains twitter api access keys and tokens.
db_properties.ini - contains all database configurations and properties.

jars:
postgresql-42.2.12.jar - contains jar required to establish a JDBC connection to postgres table.

logs:
info.log - file where info logs will be appended.
errors.log - file where errors logs will be appended.

data:
<keyword>.json - file where the tweets gets dumped.

scripts:
extract_tweets.py - Python script which reads all configuration parameters, establishes a secured Oauth connection to Twitter API, queries tweets that contains given keyword, dump the result into json file and calls the spark job to process the raw data.
spark_postgres_ETL.py - Python script which cleans, filter, transform and process the data, establishes a connection to postgres DB and write data to table.
