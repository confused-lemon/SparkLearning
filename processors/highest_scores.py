from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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


    def get_highest_scores_last_month(self):
        """Gets the highest scored posts for the last month"""
        raise NotImplementedError

    def get_highest_scores_last_week(self):
        """Gets the highest scored posts for the last week"""
        sub_query = f"""(SELECT * 
                    FROM {self.credentials['main_table']}
                    WHERE DATE(snapshot_time_utc) >= CURRENT_DATE - INTERVAL '7 days') as subquery"""

        df = self.session.read.jdbc(url=self.connection_url, table=sub_query, properties=self.connection)
        df_grouped_desc = df.select("id", "title", "subreddit", "score", "upvote_ratio", "snapshot_time_utc") \
        .orderBy(col("snapshot_time_utc").asc()) 
        
        pandas_df = df_grouped_desc.toPandas()
        pandas_df.to_csv("records.csv", index=False)

    def get_highest_scores_last_day(self):
        """Gets the highest scored posts for the last day"""
        raise NotImplementedError
