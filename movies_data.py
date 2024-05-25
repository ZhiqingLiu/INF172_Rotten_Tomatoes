import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import ast

# Database credentials and connection details
username = 'root'  
password = '12345678'  
host = 'localhost'  
port = '3306'  
database = 'Movie_database'

# Creating a database connection string
connection_string = f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(connection_string)

# Path to the CSV file
csv_file_path = '/Users/christy/Desktop/movie_dataset.csv'  # change into the own path
# Read the CSV file
df = pd.read_csv(csv_file_path)
# Filter out rows where 'audience_score' is null
df = df[df['audience_score'].notna()]
# drop the empty and unnessary column
df.drop(['critic_score', 'sound_mix', 'rating', 'box_office'], axis=1, inplace=True)

# Convert the 'audience_score' column from string representation of dictionary to actual dictionary
df['audience_score'] = df['audience_score'].apply(ast.literal_eval)

# Extract data from the dictionary and create new columns
df['audience_average_rating'] = df['audience_score'].apply(lambda x: float(x.get('average_rating')))
df['audience_banded_rating_count'] = df['audience_score'].apply(lambda x: x.get('banded_rating_count'))
df['audience_liked_count'] = df['audience_score'].apply(lambda x: int(x.get('liked_count')))
df['audience_not_liked_count'] = df['audience_score'].apply(lambda x: int(x.get('not_liked_count')))
df['audience_review_count'] = df['audience_score'].apply(lambda x: int(x.get('review_count')))
df['audience_scores'] = df['audience_score'].apply(lambda x: int(x.get('score')))
df['audience_sentiment'] = df['audience_score'].apply(lambda x: x.get('sentiment'))
df.drop('audience_score', axis=1, inplace=True)

# Convert dates
df['release_date_streaming'] = pd.to_datetime(df['release_date_streaming'], errors='coerce').dt.date
df.to_sql('movies', con=engine, if_exists='append', index=False, chunksize=500)
print("Data inserted successfully")
