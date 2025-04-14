# Python3.9
from pyspark.sql import SparkSession
from compute.daily_title_scoring import DailyTitleScoreLoad
import yaml, argparse

def main(remote: bool):
    with open('credentials.yaml', 'r') as cred_file:
        credentials = yaml.safe_load(cred_file)
        db_info = 'Database' if not remote else 'Database_Remote'

    credentials_dict = {
    'ip_addr' : credentials[db_info]['ip_addr'],
    'port' : credentials[db_info]['port'],
    'username' : credentials[db_info]['username'],
    'db' : credentials[db_info]['database'],
    'main_table' : credentials[db_info]['main_table'],
    'password' : credentials[db_info]['password']
    }

    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("RedditDataProcessor") \
        .config("spark.jars", "/usr/local/bin/postgresql-42.7.3.jar") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    connection = {
    "user" : credentials_dict['username'],
    "password" : credentials_dict['password'],
    "driver" : "org.postgresql.Driver"
    }

    score_load = DailyTitleScoreLoad(spark_session, connection, credentials_dict)
    score_load.score_and_upload()
    spark_session.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--remote', action='store_true', help='Flag to indicate connection from remote network.')
    args = parser.parse_args()
    is_remote: bool = args.remote
    main(is_remote)