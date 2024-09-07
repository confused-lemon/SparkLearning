from pyspark.sql import SparkSession
import yaml
from processors.highest_scores import HighestScoringPosts

if __name__ == "__main__":
    with open('credentials.yaml', 'r') as cred_file:
        credentials = yaml.safe_load(cred_file)

        credentials_dict = {
        'ip_addr' : credentials['Database']['ip_addr'],
        'port' : credentials['Database']['port'],
        'username' : credentials['Database']['username'],
        'db' : credentials['Database']['database'],
        'main_table' : credentials['Database']['main_table'],
        'password' : credentials['Database']['password']
        }

    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("RedditDataProcessor") \
        .config("spark.jars", "/usr/local/bin/postgresql-42.7.3.jar") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    
    connection = {
    "user" : credentials_dict['username'],
    "password" : credentials_dict['password'],
    "driver" : "org.postgresql.Driver"
    }

    driver = HighestScoringPosts(spark_session, connection, credentials_dict)
    result =  driver.get_highest_scores_last_week()
    spark_session.stop()