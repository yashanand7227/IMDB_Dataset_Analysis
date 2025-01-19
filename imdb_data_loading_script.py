import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load.dotnev()

# Retrieve credentials
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME"

# Connect to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Load Parquet files into Pandas DataFrames
fact_movies = pd.read_parquet("fact_movies.parquet")
dim_genres = pd.read_parquet("dim_genres.parquet")
dim_titles = pd.read_parquet("dim_titles.parquet")
dim_years = pd.read_parquet("dim_years.parquet")

# Write DataFrames to PostgreSQL
fact_movies.to_sql("fact_movies",engine, if_exists="replace", index=False)
dim_genres.to_sql("dim_genres", engine, if_exists="replace", index=False)
dim_titles.to_sql("dim_titles", engine, if_exists="replace", index=False)
dim_years.to_sql("dim_years", engine, if_exists="replace", index=False)

print("Data loaded successfully into PostgreSQL!")
