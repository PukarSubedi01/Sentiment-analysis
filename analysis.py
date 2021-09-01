import io
import numpy as np
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import urllib.parse as up
import psycopg2
import os
from sqlalchemy import create_engine

df = pd.read_csv('scrappedData.csv', sep='\t')
analyzer = SentimentIntensityAnalyzer()
df['neg'] = df['reviews'].apply(lambda reviews: analyzer.polarity_scores(reviews)['neg'])
df['neu'] = df['reviews'].apply(lambda reviews: analyzer.polarity_scores(reviews)['neu'])
df['pos'] = df['reviews'].apply(lambda reviews: analyzer.polarity_scores(reviews)['pos'])
df['compound'] = df['reviews'].apply(lambda reviews: analyzer.polarity_scores(reviews)['compound'])
df['category-description'] = df['compound'].apply(lambda c: 'Positive Polarized' if c > 0 else('Negative Polarized' if c < 0 else "Neutral Polarized"))
df.to_csv("analysedData",sep='\t',  index=False)


# Sending data over to elephant SQL
engine = create_engine('postgresql://lxcnsslf:MteOvQN0QLt201_x2fFi3Zkrf_PP9dgF@arjuna.db.elephantsql.com/lxcnsslf')
df.head(0).to_sql('analysis', engine, if_exists='replace', index = False) #Drops the old table and creates a new one
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output,sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, 'analysis', null="", sep = '\t')
conn.commit()
cur.close()

print("Data successfully uploaded to the cloud")
