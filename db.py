import pandas as pd
from sqlalchemy import create_engine

engine_mysql = create_engine('mysql+mysqlconnector://root:12345678@localhost:3306/Movie_database')

# SQLite database connection (creates the file if it doesn't exist)
engine_sqlite = create_engine('sqlite:///movies.db')  # This will create a 'movies.db' SQLite file in the current directory

# Read data from MySQL
query = "SELECT * FROM movies"  # Adjust this query based on your specific requirements
data = pd.read_sql(query, engine_mysql)

# Write data to SQLite
data.to_sql('movies', con=engine_sqlite, if_exists='replace', index=False)

print("Data has been exported to SQLite .db file successfully.")