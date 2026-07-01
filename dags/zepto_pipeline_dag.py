from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure the src directory is accessible to the Airflow worker environment
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import entry points from Person A and Person B production modules
try:
    from src.sentiment_pipeline import main as run_sentiment_pipeline
    # Assuming Person A's execution function follows a matching standard structural layout
    from src.review_scraper import main as run_ingestion_pipeline 
except ImportError as e:
    raise ImportError(f"Failed to import pipeline modules. Check path resolution: {e}")

# Define baseline operational configuration for the workflow tasks
default_args = {
    'owner': 'zepto_analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the main orchestration workflow container
with DAG(
    dag_id='zepto_reviews_absa_pipeline',
    default_args=default_args,
    description='Orchestrates Play Store review ingestion followed by Aspect-Based Sentiment Analysis',
    schedule_interval='0 2 * * *',  # Executes automatically every single day at 02:00 AM UTC
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Ingest new daily data (Person A's Scope)
    task_ingest_reviews = PythonOperator(
        task_id='ingest_raw_play_store_reviews',
        python_callable=run_ingestion_pipeline,
        doc_md="""### Task Description
        Scrapes raw metrics from the Google Play Store, validates the schema layout, 
        and updates the production staging table.
        """
    )

    # Task 2: Infer sentiment and aspect classifications (Person B's Scope)
    task_process_sentiment = PythonOperator(
        task_id='run_aspect_sentiment_inference',
        python_callable=run_sentiment_pipeline,
        doc_md="""### Task Description
        Pulls delta records from the staging database, runs zero-shot classification 
        and roberta sentiment analysis, and updates the final analytical output target.
        """
    )

    # Establish exact chronological dependency constraint mapping
    task_ingest_reviews >> task_process_sentiment
