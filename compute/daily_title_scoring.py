from datetime import datetime, timedelta
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StructType, StructField
from pyspark.sql import SparkSession

yesterday = datetime.now().date() - timedelta(days=1)
title_analyzer = SentimentIntensityAnalyzer()

daily_query = f"""(select distinct on (id) id, title from main_post_data 
where snapshot_time_utc::date = DATE '{yesterday}') as sub_q"""

def title_sentiment_score(title):
    scores = title_analyzer.polarity_scores(title.replace('#', ' '))
    return (scores['pos'], scores['neg'], scores['neu'])
output_schema = StructType([
    StructField("pos", FloatType(), False),
    StructField("neg", FloatType(), False),
    StructField("neu", FloatType(), False),
])
score_udf = udf(title_sentiment_score, output_schema)

class DailyTitleScoreLoad:
    def __init__(self, session: SparkSession, connection: dict, credentials: dict):
        self.session = session
        self.connection = connection
        self.credentials = credentials
        self.connection_url = f"jdbc:postgresql://{self.credentials['ip_addr']}:{self.credentials['port']}/{self.credentials['db']}"

    def score_and_upload(self):
        df = self.session.read.jdbc(url=self.connection_url, table=daily_query, properties=self.connection)
        existing_ids = self.session.read.jdbc(
            url=self.connection_url,
            table='title_sentiment_scores',
            properties=self.connection
        ).select("id").distinct()

        df = df.join(existing_ids, on='id', how='left_anti')
        df = df.withColumn("scores_tuple", score_udf(df['title']))

        df = df.withColumn("pos_scr", df["scores_tuple.pos"]) \
            .withColumn("neg_scr", df["scores_tuple.neg"]) \
            .withColumn("neu_scr", df["scores_tuple.neu"]) \
            .drop('scores_tuple', 'title')

        df.write.jdbc(url=self.connection_url, table='title_sentiment_scores', properties=self.connection, mode='append')
        del df, existing_ids