# Python3.9
from pyspark.sql import SparkSession
from processors.highest_scores import HighestScoringPosts 
from processors.title_sent_analysis import BulkTitleAnalysis
import yaml, argparse

def main(is_remote: bool):
    with open('credentials.yaml', 'r') as cred_file:
        credentials = yaml.safe_load(cred_file)
        db_info = 'Database' if not is_remote else 'Database_Remote'

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

    driver = HighestScoringPosts(spark_session, connection, credentials_dict)
    all_subs = driver.get_subs()
    all_subs.write.csv('output/subs', header=True, mode='overwrite')

    election_analysis = BulkTitleAnalysis(spark_session, connection, credentials_dict)
    election_post_data = election_analysis.sent_analysis_of_political_posts_six_weeks()
    election_post_data.write.csv('output/six_weeks_election', header=True, mode='overwrite')
    # month_result = driver.get_highest_scores_last_month()
    
    # result =  driver.get_highest_scores_last_week()
    # month_results = driver.get_highest_scores_last_month()
    # result.show()
    # month_results.coalesce(1).write.csv('output/', header=True, mode='overwrite')
    # title_analysis = BulkTitleAnalysis(spark_session, connection, credentials_dict)
    # title_analysis.break_into_year_months()
    spark_session.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--remote', action='store_true', help='Flag to indicate connection from remote network.')
    args = parser.parse_args()
    is_remote: bool = args.remote
    main(is_remote)