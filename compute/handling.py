import platform, os
from pathlib import Path
from glob import glob
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer

if platform.system() == "Windows": # windows was being annoying
    home_dir = Path.home()
    csv_path = home_dir.parent / "data" / "output" / "six_weeks_election" / "*.csv"
    csv_files = glob(str(csv_path))
else:
    path = os.path.expanduser("~/data/output/six_weeks_election/*.pdf")
    csv_files = glob(path)
    if len(csv_files) == 0:
        raise ValueError(f"No .csv files found in dir: {path}")

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

unique_titles = master_frame[['id', 'title', 'snapshot_time_utc']].drop_duplicates(subset=['id'])
scored_titles = unique_titles.apply(lambda row: title_sentiment_analysis_scores(row), axis=1)