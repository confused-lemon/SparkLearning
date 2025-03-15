import sys, os
import numpy as np
import pandas as pd
from glob import glob
from nltk.sentiment import SentimentIntensityAnalyzer

csv_files = glob('output/six_weeks_election/*.csv')
title_analizer = SentimentIntensityAnalyzer()

def title_sentiment_analysis_scores(self, title: str):
        """Returns sentiment analysis of a given post title"""
        return title_analizer.polarity_scores(title.replace('#', ' ')) # polarity_scores ignores hashtagged words

dfs = []
for file in csv_files:
    df = pd.read_csv(file, header =0)
    dfs.append(df)
master_frame = pd.concat(dfs, ignore_index=True)


master_frame.sort_values("score").groupby("id")