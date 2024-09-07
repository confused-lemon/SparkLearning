from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from datetime import datetime
import pandas as pd

class HighestScoringPosts:
    def __init__(self, session: SparkSession, connection: dict, credentials: dict):
        self.session = session
        self.connection = connection
        self.credentials = credentials
        self.today = datetime.today().strftime('%Y-%m-%d')
        self.connection_url = f"jdbc:postgresql://{self.credentials['ip_addr']}:{self.credentials['port']}/{self.credentials['db']}"

    def get_highest_scores_all_time(self):
        """Gets the highest scores for all posts"""

        sub_query = '''SELECT id, score, num_comments, upvote_ratio, snapshot_time_utc, COUNT(*)
                    FROM main_post_data 
                    GROUP BY rank, id, score, num_comments, upvote_ratio, snapshot_time_utc
                    HAVING COUNT(*) > 1;'''

        df = self.session.read.jdbc(url=self.connection_url, table=self.credentials['main_table'], properties=self.connection)
        df_grouped_desc = df.groupBy("rank", "id", "score", "num_comments", "upvote_ratio", "snapshot_time_utc") \
               .agg(count("*").alias("duplicate_cnt")) \
               .filter("duplicate_cnt > 1") \
               .orderBy(col("snapshot_time_utc").desc())
        
        df_grouped_asc = df.groupBy("rank", "id", "score", "num_comments", "upvote_ratio", "snapshot_time_utc") \
               .agg(count("*").alias("duplicate_cnt")) \
               .filter("duplicate_cnt > 1") \
               .orderBy(col("snapshot_time_utc").asc())
        
        pandas_df = df_grouped_desc.toPandas()
        pandas_df.to_csv("duplicated_records_desc.csv", index=False)

        pandas_df = df_grouped_asc.toPandas()
        pandas_df.to_csv("duplicated_records_asc.csv", index=False)


    def get_highest_scores_last_month(self):
        """Gets the highest scored posts for the last month"""
        raise NotImplementedError

    def get_highest_scores_last_week(self):
        """Gets the highest scored posts for the last week"""
        raise NotImplementedError

    def get_highest_scores_last_day(self):
        """Gets the highest scored posts for the last day"""
        raise NotImplementedError
