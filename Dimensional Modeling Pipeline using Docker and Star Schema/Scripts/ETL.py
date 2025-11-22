import pandas as pd
import mysql.connector
import os
from typing import Optional

DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
RAW_DATA_PATH = '/app/data/shopping_trends.csv'

TABLES = {
    'customer': 'Dim_Customer',
    'item': 'Dim_Item',
    'purchase': 'Fact_Purchase'
}

def get_create_table_queries():
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
            payment_method VARCHAR(50),
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
    print("1. Starting data reading and normalization...")
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.lower().str.replace('[^a-z0-9_]+', '_', regex=True)
        df.rename(columns={'purchase_amount_usd_': 'purchase_amount_usd'}, inplace=True)
        df['purchase_transaction_id'] = df.index

        dim_customer = df[['customer_id', 'age', 'gender', 'location', 'subscription_status', 'frequency_of_purchases']].drop_duplicates(subset=['customer_id'])
        print(f"   -> Dim_Customer size: {len(dim_customer)}")

        dim_item = df[['item_purchased', 'category', 'size', 'color', 'season']].drop_duplicates(subset=['item_purchased', 'category'])
        dim_item.rename(columns={'item_purchased': 'item_name'}, inplace=True)
        print(f"   -> Dim_Item size: {len(dim_item)}")

        fact_purchase = df[['purchase_transaction_id', 'customer_id', 'item_purchased', 'category',
                            'purchase_amount_usd', 'review_rating', 'shipping_type', 'discount_applied',
                            'promo_code_used', 'previous_purchases', 'preferred_payment_method']].copy()

        fact_purchase.rename(columns={'item_purchased': 'item_name', 'preferred_payment_method': 'payment_method'}, inplace=True)

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
    print("2. Setting up target tables...")
    drop_order = [TABLES['purchase'], TABLES['customer'], TABLES['item']]
    for table_name in drop_order:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            print(f"Could not drop table {table_name}: {e}")

    for query in get_create_table_queries():
        cursor.execute(query)
    print("All tables created successfully.")

def load_data(data_dict: dict):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        cursor = conn.cursor()
        setup_database(cursor)

        for table_key, table_name in TABLES.items():
            df = data_dict[table_name]
            cols = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            records_to_insert = [tuple(row) for row in df.values]

            print(f"3. Loading {len(records_to_insert)} records into {table_name}...")
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

if __name__ == "__main__":
    normalized_data = read_and_normalize_data(RAW_DATA_PATH)
    if normalized_data is not None:
        load_data(normalized_data)
