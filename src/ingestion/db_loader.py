import os
import logging
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_reviews(reviews):
    if not reviews:
        logging.warning("No reviews to insert.")
        return 0

    conn = None
    cursor = None

    db_host = os.environ.get("DB_HOST", "localhost")
    db_name = os.environ.get("DB_NAME", "playstore_db")
    db_user = os.environ.get("DB_USER", "postgres")
    db_pass = os.environ.get("DB_PASS", "postgres")

    logging.info("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_pass
        )
        logging.info("Database connection established.")
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return 0

    inserted_count = 0

    try:
        logging.info(f"Loading {len(reviews)} reviews...")
        
        # Idempotent query
        insert_query = """
            INSERT INTO reviews (review_id, user_name, content, score, at)
            VALUES %s
            ON CONFLICT (review_id) DO NOTHING;
        """
        
        # Tuple mapping
        data_tuples = [
            (r['reviewId'], r['userName'], r['content'], r['score'], r['at']) 
            for r in reviews
        ]

        psycopg2.extras.execute_values(
            cursor,          # DB cursor
            insert_query,    # SQL string
            data_tuples,     # Batch data
            page_size=100    # Chunk size
        )
        
        inserted_count = cursor.rowcount
        
        if inserted_count < len(reviews):
            logging.warning(f"Duplicate reviews skipped by database. Inserted {inserted_count} out of {len(reviews)}.")
        else:
            logging.info(f"Successfully inserted {inserted_count} reviews.")

        conn.commit()
        logging.info("Transaction committed.")

    except Exception as e:
        logging.error(f"Insert failed: {e}")
        if conn:
            conn.rollback()
        logging.info("Rolling back transaction.")
        inserted_count = 0

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    return inserted_count

if __name__ == "__main__":
    from playstore_scraper import fetch_reviews
    
    test_app = "com.zeptoconsumerapp" 
    
    scraped_data = fetch_reviews(test_app, review_count=10)
    rows_inserted = load_reviews(scraped_data)
    
    print(f"Test inserted rows: {rows_inserted}")