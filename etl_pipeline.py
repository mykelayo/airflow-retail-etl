import pandas as pd
import os
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'dbname': 'retail_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

def extract_data(file_path):
    """Extract data from a CSV file."""
    print("Extracting data from CSV...")
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully extracted {len(df)} records.")
        return df
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise

def transform_data(df):
    """Transform and clean the data."""
    print("Transforming data...")
    
    # Handle missing values
    df['price'] = df['price'].fillna(df['price'].mean())  # Fill missing prices with mean
    df['quantity'] = df['quantity'].clip(lower=1)  # Replace zero/negative quantities with 1
    
    # Standardize date format
    df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d')
    
    # Add a new column for total sales
    df['total_sales'] = df['price'] * df['quantity']
    
    print("Data transformation complete.")
    return df

def load_data(df):
    """Load data into PostgreSQL database."""
    print("Loading data to PostgreSQL...")
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sales (
            order_id INTEGER PRIMARY KEY,
            order_date DATE,
            product VARCHAR(100),
            category VARCHAR(50),
            price FLOAT,
            quantity INTEGER,
            customer_id VARCHAR(10),
            total_sales FLOAT
        );
        """
        cursor.execute(create_table_query)
        
        # Insert data
        for _, row in df.iterrows():
            insert_query = sql.SQL("""
            INSERT INTO sales (order_id, order_date, product, category, price, quantity, customer_id, total_sales)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                order_date = EXCLUDED.order_date,
                product = EXCLUDED.product,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                quantity = EXCLUDED.quantity,
                customer_id = EXCLUDED.customer_id,
                total_sales = EXCLUDED.total_sales
            """)
            cursor.execute(insert_query, (
                row['order_id'],
                row['order_date'],
                row['product'],
                row['category'],
                row['price'],
                row['quantity'],
                row['customer_id'],
                row['total_sales']
            ))

        conn.commit()
        print(f"Successfully loaded {len(df)} records to database.")
    
    except Exception as e:
        print(f"Error during loading: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def run_etl():
    """Run the full ETL pipeline."""
    file_path = os.path.join(os.path.dirname(__file__), 'sales_data.csv')
    df = extract_data(file_path)
    df_transformed = transform_data(df)
    load_data(df_transformed)
    print("ETL pipeline completed successfully.")

if __name__ == "__main__":
    run_etl()