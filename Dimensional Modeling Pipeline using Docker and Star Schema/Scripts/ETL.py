import pandas as pd
import mysql.connector
import os
from typing import Optional

# --- Configuration (Pulled from Docker Compose Environment Variables) ---
DB_HOST = os.environ.get('DB_HOST')        
DB_USER = os.environ.get('DB_USER')        
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')        
RAW_DATA_PATH = '/app/data/shopping_trends.csv' 

# --- Target Tables ---
TABLES = {
    'customer': 'Dim_Customer',
    'item': 'Dim_Item',
    'purchase': 'Fact_Purchase'
}

# --- Schema Definitions (Used for table creation) ---
def get_create_table_queries():
    """Returns the SQL queries to create the normalized tables."""
    return [
        """
        CREATE TABLE Dim_Customer (
            customer_id INT NOT NULL,
            age INT,
            gender VARCHAR(10),
            location VARCHAR(50),
            subscription_status VARCHAR(10),
            frequency_of_purchases VARCHAR(50),
            PRIMARY KEY (customer_id)
        );
        """,
        """
        CREATE TABLE Dim_Item (
            item_name VARCHAR(50) NOT NULL,
            category VARCHAR(50) NOT NULL,
            size VARCHAR(5),
            color VARCHAR(20),
            season VARCHAR(20),
            PRIMARY KEY (item_name, category)
        );
        """,
        """
        CREATE TABLE Fact_Purchase (
            purchase_transaction_id INT NOT NULL,
            customer_id INT NOT NULL,
            item_name VARCHAR(50) NOT NULL,
            category VARCHAR(50) NOT NULL,
            purchase_amount_usd DECIMAL(10, 2),
            review_rating DECIMAL(3, 2),
            payment_method VARCHAR(50),      -- ONLY ONE PAYMENT METHOD FIELD IS ALLOWED HERE
            shipping_type VARCHAR(50),
            discount_applied VARCHAR(5),
            promo_code_used VARCHAR(5),
            previous_purchases INT,
            
            PRIMARY KEY (purchase_transaction_id),
            FOREIGN KEY (customer_id) REFERENCES Dim_Customer(customer_id),
            FOREIGN KEY (item_name, category) REFERENCES Dim_Item(item_name, category)
        );
        """
    ]

def read_and_normalize_data(file_path: str) -> Optional[dict]:
    """Reads CSV, normalizes columns, and splits data into Dim/Fact DataFrames."""
    print("1. Starting data reading and normalization...")
    try:
        df = pd.read_csv(file_path)
        
        # --- Normalize ALL column names to snake_case first ---
        df.columns = df.columns.str.lower().str.replace('[^a-z0-9_]+', '_', regex=True)
        
        # --- CRITICAL FIX 1: Explicitly rename the Purchase Amount column ---
        # Rename the likely snake-cased column 'purchase_amount_usd_' to the expected 'purchase_amount_usd'
        df.rename(columns={'purchase_amount_usd_': 'purchase_amount_usd'}, inplace=True)
        
        # --- Create Transaction ID ---
        df['purchase_transaction_id'] = df.index
        
        # --- 1. Dim_Customer (Get unique customer data) ---
        dim_customer = df[[
            'customer_id', 'age', 'gender', 'location', 
            'subscription_status', 'frequency_of_purchases'
        ]].drop_duplicates(subset=['customer_id'])
        print(f"   -> Dim_Customer size: {len(dim_customer)}")

        # --- 2. Dim_Item (Get unique item/category combinations) ---
        dim_item = df[[
            'item_purchased', 'category', 'size', 'color', 'season'
        ]].drop_duplicates(subset=['item_purchased', 'category'])
        dim_item.rename(columns={'item_purchased': 'item_name'}, inplace=True)
        print(f"   -> Dim_Item size: {len(dim_item)}")

        # --- 3. Fact_Purchase (All transaction data and foreign keys) ---
        # CRITICAL FIX 2: Only select 'preferred_payment_method' to avoid column duplication.
        fact_purchase = df[[
            'purchase_transaction_id', 
            'customer_id', 
            'item_purchased', 
            'category',
            'purchase_amount_usd',  
            'review_rating', 
            'shipping_type', 
            'discount_applied', 
            'promo_code_used', 
            'previous_purchases', 
            'preferred_payment_method' # Use this one to be renamed below
            # 'payment_method' (the original one) is explicitly omitted
        ]].copy()
        
        # Rename columns to match the SQL schema
        fact_purchase.rename(columns={
            'item_purchased': 'item_name', 
            # CRITICAL FIX 3: Rename the selected column to the required target name.
            'preferred_payment_method': 'payment_method' 
        }, inplace=True)

        print(f"Data normalized. Fact table size: {len(fact_purchase)} rows.")
        
        return {
            'Dim_Customer': dim_customer,
            'Dim_Item': dim_item,
            'Fact_Purchase': fact_purchase
        }
        
    except FileNotFoundError:
        print(f"Error: Input file not found at {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred during data normalization: {e}")
        return None


def setup_database(cursor):
    """Drops and creates all three tables in the correct order."""
    print("2. Setting up target tables...")
    # Drop in reverse order to respect foreign key constraints
    drop_order = [TABLES['purchase'], TABLES['customer'], TABLES['item']]
    for table_name in drop_order:
        try:
            # We use IF EXISTS because the table might not exist on the first run
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            print(f"Could not drop table {table_name}: {e}")

    # Create in correct order (Dimensions first, then Fact)
    for query in get_create_table_queries():
        cursor.execute(query)
    print("All tables created successfully.")


def load_data(data_dict: dict):
    """Connects to MySQL and loads all DataFrames."""
    try:
        # Establish connection
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        cursor = conn.cursor()
        
        # 1. Setup the Schema
        setup_database(cursor)

        # 2. Load Data (Load Dimensions first, then Fact)
        # Load order must respect foreign key constraints: Customer, Item, then Purchase
        for table_key, table_name in TABLES.items():
            df = data_dict[table_name]
            
            # Use columns from the DataFrame for the INSERT statement
            cols = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            
            # Prepare records for insertion
            records_to_insert = [tuple(row) for row in df.values]

            print(f"3. Loading {len(records_to_insert)} records into {table_name}...")
            # Use executemany for efficient batch insertion
            cursor.executemany(insert_query, records_to_insert)
            conn.commit()
            print(f"   -> {table_name} load complete.")

        print("\nSUCCESS: All data loaded and committed!")

    except mysql.connector.Error as err:
        print(f"\nCRITICAL DATABASE ERROR: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

# --- Main Execution Block ---
if __name__ == "__main__":
    normalized_data = read_and_normalize_data(RAW_DATA_PATH)
    if normalized_data is not None:
        load_data(normalized_data)