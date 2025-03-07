from datetime import datetime
from pyspark.sql.functions import col, max, year, month, asc
from pyspark.sql import SparkSession
from nltk.sentiment import SentimentIntensityAnalyzer

# Ideas: scores based on position, time on 100
# score based on median pos/neg/neu score of title
# pos/neg/neu score leading to election / reactions
# scores based on CEO killing in NY
# pos/neu/neg/compound
# needed: id, rank, title, score, ratio, comments, 

class BulkTitleAnalysis:
    """Analizes sentimentof a a title, scores it at a monthly basis"""

    monthly_dataframes = []

    def __init__(self, session: SparkSession, connection: dict, credentials: dict):
        self.session = session
        self.connection = connection
        self.credentials = credentials
        self.today = datetime.today().strftime('%Y-%m-%d')
        self.connection_url = f"jdbc:postgresql://{self.credentials['ip_addr']}:{self.credentials['port']}/{self.credentials['db']}"
        self.title_analizer = SentimentIntensityAnalyzer()
    
    def title_sentiment_analysis_scores(self, title: str):
        """Returns sentiment analysis of a given post title"""
        return self.title_analizer.polarity_scores(title.replace('#', ' ')) # polarity_scores ignores hashtagged words
    
    def break_into_year_months(self):
        """Breaks data into year/month blocks for grouped analysis"""
        df = self.session.read.jdbc(url=self.connection_url, table='main_post_data', properties=self.connection)
        year_month = df.withColumn("year", year(col('snapshot_time_utc')))\
                        .withColumn("month", month(col("snapshot_time_utc")))
        years_and_months = year_month.select("year", "month").distinct().collect()
        return sorted(years_and_months)
    
    def sent_analysis_of_political_posts_six_weeks(self):
        #subs: 'news', 'worldnews', 'geopolitics', 'nottheonion', 'politics', 'internationalpolitics', 'democrats', 'conservative'
        sub_q = f"""(select * from {self.credentials['main_table']} 
        where subreddit in ('news', 'worldnews', 'geopolitics', 'nottheonion', 'politics', 'internationalpolitics', 'democrats', 'conservative')
        and date(snapshot_time_utc) >= '2024-09-23' and date(snapshot_time_utc) <= '2024-12-16') as sub_query"""
        return self.session.read.jdbc(url=self.connection_url, table=sub_q, properties=self.connection) \
            .sort(asc('snapshot_time_utc'))
        