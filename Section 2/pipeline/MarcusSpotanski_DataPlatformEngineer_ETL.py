import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# Transformation and Cleaning process for Sales Data
# Focuses on ensuring unique identifiers, consistent formatting, and easy to 
def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    
    # Standardize column names to all uppercase, stripped of leading/trailing spaces, with spaces converted to underscores
    df.columns = df.columns.astype(str).str.strip().str.upper().str.replace(' ', '_')
    
    # Clean text columns by removing outer whitespace and normalizing case
    text_columns = df.select_dtypes(include=['object', 'string']).columns
    for col in text_columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
        # Convert literal text 'NAN', 'NONE', or empty strings into true missing values (NaN)
        df[col] = df[col].replace({'NAN': np.nan, 'NONE': np.nan, '': np.nan})

    # Convert timestamp values to UTC to resolve mixed timezone formats cleanly
    parsed_dates = pd.to_datetime(df['ORDER_TS'], format='mixed', utc=True, errors='coerce')
    
    # Extract the calendar date (e.g., 2025-10-01) into a separate column
    df['ORDER_DATE'] = parsed_dates.dt.date
    
    # Extract the time component (e.g., 07:42:56) if present, otherwise set as missing
    df['ORDER_TIME'] = parsed_dates.dt.time
    
    # Drop original unparsed timestamp column to simplify data model
    df = df.drop(columns=['ORDER_TS'])

    # Strip currency symbols and formatting from price fields and cast to float
    if 'UNIT_PRICE_USD' in df.columns:
        df['UNIT_PRICE_USD'] = (
            df['UNIT_PRICE_USD']
            .astype(str)
            .str.replace(r'[\$,]', '', regex=True)
            .astype(float)
            .round(2)
        )

    # Clean shipping days by parsing to numbers and substituting missing values with 0
    if 'SHIPPING_DAYS' in df.columns:
        df['SHIPPING_DAYS'] = pd.to_numeric(df['SHIPPING_DAYS'], errors='coerce').fillna(0).astype(int)

    # Ensure quantity is cast to numeric integer format without altering positive/negative values
    if 'QUANTITY' in df.columns:
        df['QUANTITY'] = pd.to_numeric(df['QUANTITY'], errors='coerce').fillna(0).astype(int)

    # Ensure discount percentage is cast to float without modifying out-of-bounds rates
    if 'DISCOUNT_PCT' in df.columns:
        df['DISCOUNT_PCT'] = pd.to_numeric(df['DISCOUNT_PCT'], errors='coerce').fillna(0.0)

    # Return the full dataset without dropping any rows or altering core values
    return df


# Automatically provisions the target database table in PostgreSQL if missing
def create_tables_if_not_exists(engine):
    create_fct_sales_table = """
    CREATE TABLE IF NOT EXISTS fct_sales_data (
        order_id VARCHAR(255),
        customer_id VARCHAR(255),
        region VARCHAR(100),
        channel VARCHAR(100),
        product_sku VARCHAR(255),
        product_name VARCHAR(255),
        category VARCHAR(100),
        quantity INT,
        unit_price_usd NUMERIC(10, 2),
        discount_pct NUMERIC(5, 4),
        order_status VARCHAR(100),
        shipping_days INT,
        order_date DATE,
        order_time TIME
    );
    """
    with engine.begin() as connection:
        connection.execute(text(create_fct_sales_table))


# Establishes database connection using system environment variables
def get_db_engine():
    db_host = os.getenv("PGHOST")
    db_port = os.getenv("PGPORT", "5432")
    db_name = os.getenv("PGDATABASE")
    db_user = os.getenv("PGUSER", "postgres")
    db_password = os.getenv("PGPASSWORD")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_string)


# Ingests processed sales records into PostgreSQL database
def load_sales_data_to_postgres(df: pd.DataFrame):
    engine = get_db_engine()
    create_tables_if_not_exists(engine)
    
    # Convert column headers to lowercase to standard PostgreSQL naming conventions
    df.columns = df.columns.str.lower()

    if not df.empty:
        df.to_sql(
            name='fct_sales_data',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f"Successfully loaded {len(df)} rows into 'fct_sales_data'.")


# Main Method
# assumes the csv file used below is in current directory
if __name__ == "__main__":
    # Update path and filename if changed
    path = "data/"
    file = "dpe_interview_takehome_data.csv"
    raw_df = pd.read_csv(path + file)
    
    # Execute ETL Pipeline
    processed_df = transform_sales_data(raw_df)
    
    # Upload cleaned data into PostgresSQL Table
    load_sales_data_to_postgres(processed_df)