from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

# Ideas: scores based on position, time on 100
# score based on median pos/neg/neu score of title
# pos/neg/neu score leading to election / reactions

