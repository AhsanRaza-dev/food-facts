import firebase_admin
from firebase_admin import credentials, firestore
import psycopg2
import os
import json
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv(r'd:\curiologix\barcode\.env')

# DB Config
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

SERVICE_ACCOUNT_KEY = r'd:\curiologix\barcode\serviceAccountKey.json'

def get_pg_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def init_firebase():
    if not os.path.exists(SERVICE_ACCOUNT_KEY):
        print(f"Error: Service account key not found at {SERVICE_ACCOUNT_KEY}")
        print("Please download it from Firebase Console and place it in the folder.")
        return None
    
    try:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        return None


import re

# Valid chars for Firestore keys: No dots (.), no slashes (/), no [ ] * ~
def sanitize_data(data):
    """Recursively cleans dictionary keys to be Firestore-safe."""
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            # Replace invalid chars with underscore
            # Keep only alphanumeric and underscore
            clean_key = re.sub(r'[^a-zA-Z0-9_]', '_', str(k))
            
            # Key cannot be empty or start with __ (double underscore reserved)
            if not clean_key or clean_key.startswith('__'):
                clean_key = 'x_' + clean_key
                
            new_data[clean_key] = sanitize_data(v)
        return new_data
    elif isinstance(data, list):
        return [sanitize_data(i) for i in data]
    else:
        return data

def upload_single_doc(db, collection, barcode, brand_name, product_data_raw):
    """Try to upload a single document. If sanitization fails, upload as string."""
    doc_ref = db.collection(collection).document(barcode)
    
    # JSON Parse
    if isinstance(product_data_raw, str):
        try:
            data_obj = json.loads(product_data_raw)
        except:
            data_obj = {"raw_text": product_data_raw}
    else:
        data_obj = product_data_raw

    # Attempt 1: Sanitize and Upload as Object
    try:
        clean_data = sanitize_data(data_obj)
        doc_data = {
            'barcode': barcode,
            'brand_name': brand_name,
            'product_data': clean_data,
            'migrated_at': firestore.SERVER_TIMESTAMP,
            'data_format': 'json_object'
        }
        doc_ref.set(doc_data)
        return True
    except Exception as e:
        print(f"Warning: Failed to upload {barcode} as object ({e}).")
        
    # Attempt 2: Upload as String (Fallback)
    try:
        if not isinstance(product_data_raw, str):
            json_str = json.dumps(product_data_raw)
        else:
            json_str = product_data_raw
            
        doc_data = {
            'barcode': barcode,
            'brand_name': brand_name,
            'product_data_json': json_str, # Store as string
            'migrated_at': firestore.SERVER_TIMESTAMP,
            'data_format': 'json_string_fallback'
        }
        doc_ref.set(doc_data)
        print(f"Recovered: Uploaded {barcode} as JSON string.")
        return True
    except Exception as e2:
        print(f"Error: Could not upload {barcode} even as string: {e2}")
        return False

def migrate():
    # 1. Connect to PostgreSQL
    pg_conn = get_pg_connection()
    if not pg_conn:
        return

    # 2. Connect to Firestore
    db = init_firebase()
    if not db:
        pg_conn.close()
        return

    print("Connected to PostgreSQL and Firebase. Starting migration...")

    # 3. Stream data from PG
    BATCH_SIZE = 50 
    PRODUCTS_COLLECTION = 'products'
    
    total_migrated = 0
    start_time = time.time()

    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT barcode, brand_name, product_data FROM mapped_products")
            
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                
                # Try Batch Write first
                batch = db.batch()
                current_batch_rows = [] 
                
                for row in rows:
                    barcode = row[0]
                    brand_name = row[1] 
                    product_data = row[2]
                    
                    if not barcode:
                        continue
                        
                    current_batch_rows.append((barcode, brand_name, product_data))

                    # Prepare Sanitized Data for Batch
                    if isinstance(product_data, str):
                        try: product_data_obj = json.loads(product_data)
                        except: product_data_obj = {}
                    else:
                        product_data_obj = product_data
                    
                    clean_data = sanitize_data(product_data_obj)
                    
                    doc_ref = db.collection(PRODUCTS_COLLECTION).document(barcode)
                    doc_data = {
                        'barcode': barcode,
                        'brand_name': brand_name,
                        'product_data': clean_data,
                        'migrated_at': firestore.SERVER_TIMESTAMP,
                        'data_format': 'json_object'
                    }
                    batch.set(doc_ref, doc_data)
                
                try:
                    batch.commit()
                    total_migrated += len(current_batch_rows)
                    elapsed = time.time() - start_time
                    print(f"Migrated {total_migrated} records... ({elapsed:.2f}s elapsed)")
                except Exception as batch_error:
                    print(f"Batch failed ({batch_error}). Retrying items individually...")
                    
                    # Fallback: Single Item Upload
                    for b_barcode, b_brand, b_data in current_batch_rows:
                         if upload_single_doc(db, PRODUCTS_COLLECTION, b_barcode, b_brand, b_data):
                             total_migrated += 1
                    
                    elapsed = time.time() - start_time
                    print(f"Recovered batch. Total: {total_migrated}")

    except Exception as e:
        print(f"Migration loop failed: {e}")
    finally:
        pg_conn.close()
        print(f"Migration finished. Total documents uploaded: {total_migrated}")

if __name__ == "__main__":
    migrate()
