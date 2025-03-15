import platform, os
from pathlib import Path
import pandas as pd
from glob import glob
from nltk.sentiment import SentimentIntensityAnalyzer

if platform.system() == "Windows": # windows was being annoying
    script_location = Path(__file__).resolve().parent
    csv_path = script_location.parent / "output" / "six_weeks_election" / "*.csv"
    csv_files = glob(str(csv_path))
else:
    csv_files = glob('output/six_weeks_election/*.csv')
title_analyzer = SentimentIntensityAnalyzer()
def title_sentiment_analysis_scores(row):
        """Adds sentiment analysis of a given post title to the unique Id frame"""
        scores = title_analyzer.polarity_scores(row['title'].replace('#', ' ')) # polarity_scores ignores hashtagged words
        return pd.Series(
            {'id': row['id'],
            'title': row['title'],
            'pos': scores['pos'],
            'neg': scores['neg'],
            'neu': scores['neu']})

dfs = []
for file in csv_files:
    df = pd.read_csv(file, header =0)
    dfs.append(df)
master_frame = pd.concat(dfs, ignore_index=True)

unique_titles = master_frame[['id', 'title', 'snapshot_time_utc']].drop_duplicates()

scored_titles = unique_titles.apply(lambda row: title_sentiment_analysis_scores(row), axis=1)