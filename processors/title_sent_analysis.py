from datetime import datetime
from pyspark.sql.functions import col, max, year, month
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
        return self.title_analizer.polarity_scores(title.replace('#', ' ')) # polarity_scores ignores #'s
    
    def break_into_year_months(self):
        """Breaks data into year/month blocks for grouped analysis"""
        df = self.session.read.jdbc(url=self.connection_url, table='main_post_data', properties=self.connection)
        year_month = df.withColumn("year", year(col('snapshot_time_utc')))\
                        .withColumn("month", month(col("snapshot_time_utc")))
        years_and_months = year_month.select("year", "month").distinct().collect()
        for row in years_and_months:
            y, m = row['year'], row['month']
            print(f"Year: {y}, Month: {m}")