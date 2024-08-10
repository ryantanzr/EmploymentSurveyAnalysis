from dotenv import load_dotenv
import os, logging
import pandas as pd
import sqlalchemy

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuring panda display
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 10)
pd.set_option("display.width", 50)
pd.set_option("display.precision", 2)

# Database connection
load_dotenv()
server="localhost"
pwd = os.getenv("DB_PASSWORD")
uid = os.getenv("DB_USER")

# Create connection string
conn_string = f"postgresql://{uid}:{pwd}@{server}:5432/graduate_employment_survey"
engine = sqlalchemy.create_engine(url=conn_string, pool_pre_ping=True)

try:
    conn = engine.connect()
except sqlalchemy.exc.SQLAlchemyError as e:
    logging.error("Database connection failed: %s", e)
    raise

# Extract data from csv file
def extract(file_path) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path)
        logging.info("Data extracted successfully from %s", file_path)
        return df
    except FileNotFoundError as e:
        logging.error("File not found: %s", e)
        raise
    except pd.errors.ParserError as e:
        logging.error("Error parsing CSV file: %s", e)
        raise

# Perform data cleaning and metadata manipulation
def standardize(df: pd.DataFrame) -> pd.DataFrame:
  
    # Keep numeric columns for metadata manipulation
    cols = df.columns.drop(['year', 'university', 'school', 'degree'])
    df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")

    # Drop degree column for further calculations
    df = df.drop(columns=["degree"])
    
    # Clean the data by removing rows with missing values and rounding to 2 decimal places
    results = df.dropna(axis=0, how="any").round(2)

    logging.info("Data standardized successfully")
    
    return results


# Enrich the data by calculating the mean of each group
# and adding a timestamp to the data for the last 
# modification
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    
    # Group the data by university, school and year, and
    # calculate the mean of each group, then add suffix "_mean"
    results = df.groupby(["year", "university", "school"]).mean(numeric_only=True).add_suffix("_mean").reset_index().round(2)
    results["last_updated"] = pd.Timestamp.now()

    logging.info("Data enriched successfully")
    
    return results

# Load data to a postgres database in 3 layers: raw, standardised, enriched
def load(layer, df: pd.DataFrame):
    table_name = f"{layer}_layer"
    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info("Data loaded to layer: %s", layer)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error("Failed to load data to %s: %s", layer, e)
        raise

file_path = "GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD.csv"
df = extract(file_path)
df_cleaned = standardize(df.copy(deep=True))

load("raw", df)
load("standardized", df_cleaned)
load("enriched", enrich(df_cleaned.copy(deep=True)))