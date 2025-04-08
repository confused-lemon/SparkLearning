import os
from sys import argv
import yaml
from datetime import datetime

from jedi.inference.gradual.typing import Tuple
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import col, max, year, month, asc, udf
from pyspark.sql.types import FloatType, StructType, StructField
from pyspark.sql import SparkSession

title_analyzer = SentimentIntensityAnalyzer()
def title_sentiment_score(title):
    scores = title_analyzer.polarity_scores(title.replace('#', ' '))
    return (scores['pos'], scores['neg'], scores['neu'])
output_schema = StructType([
    StructField("pos", FloatType(), False),
    StructField("neg", FloatType(), False),
    StructField("neu", FloatType(), False),
])
score_udf = udf(title_sentiment_score, output_schema)

try:
    with open('credentials.yaml', 'r') as cred_file:
        credentials = yaml.safe_load(cred_file)
except FileNotFoundError:
    print('in except block')
    os.chdir(os.path.expanduser('~/Desktop/Code/Projects/SparkLearning'))
    with open('credentials.yaml', 'r') as cred_file:
        credentials = yaml.safe_load(cred_file)

db_info = 'Database' if ('-e' not in argv) else 'Database_Remote'
credentials_dict = {
            'ip_addr': credentials[db_info]['ip_addr'],
            'port': credentials[db_info]['port'],
            'username': credentials[db_info]['username'],
            'db': credentials[db_info]['database'],
            'main_table': credentials[db_info]['main_table'],
            'password': credentials[db_info]['password']
        }

spark_session = SparkSession.builder \
    .master("local[*]") \
    .appName("TitleSentimentAnalysis") \
    .config("spark.jars", "/usr/local/bin/postgresql-42.7.3.jar") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

connection = {
    "user": credentials_dict['username'],
    "password": credentials_dict['password'],
    "driver": "org.postgresql.Driver"
}

samp_query = '(SELECT id, title from sandbox_data_table) as sub_q' #table for testing purposes

connection_url = f"jdbc:postgresql://{credentials_dict['ip_addr']}:{credentials_dict['port']}/{credentials_dict['db']}"

df = spark_session.read.jdbc(url=connection_url, table=samp_query, properties=connection)
df = df.dropDuplicates(subset=['id']) \
    .withColumn("scores_tuple", score_udf(df['title']))

df = df.withColumn("pos_scr", df["scores_tuple.pos"]) \
    .withColumn("neg_scr", df["scores_tuple.neg"]) \
    .withColumn("neu_scr", df["scores_tuple.neu"]) \
    .drop('scores_tuple', 'title')


df.write.jdbc(url=connection_url, table='title_sentiment_scores', properties=connection, mode='append')