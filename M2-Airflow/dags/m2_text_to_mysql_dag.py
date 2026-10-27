from datetime import datetime, timedelta
import csv
import smtplib
from email.mime.text import MIMEText
from airflow import DAG
from airflow.operators.python import PythonOperator
import mysql.connector

default_args = {
    'owner': 'f1arl_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def load_text_to_mysql():
    conn = mysql.connector.connect(
        host="mysql",
        user="airflow_user",
        password="airflow_pass",
        database="f1arl_db"
    )
    cursor = conn.cursor()

    # 1. Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS company_records (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(100),
            joining_date DATE
        );
    """)

    # 2. Ingest CSV idempotently
    with open('/opt/airflow/data/company_records.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute("""
                INSERT INTO company_records (id, name, department, joining_date)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    name = VALUES(name),
                    department = VALUES(department),
                    joining_date = VALUES(joining_date);
            """, (row['id'], row['name'], row['department'], row['joining_date']))

    conn.commit()
    cursor.close()
    conn.close()
    print("SUCCESS: Records loaded into MySQL successfully.")

def send_completion_email():
    # Required syllabus message string
    message_text = "Airflow Job Complete - Text file Loaded into MYSQL"
    
    print("=" * 60)
    print(f"[STATUS NOTIFICATION]: {message_text}")
    print("=" * 60)

with DAG(
    dag_id='m2_text_to_mysql_email_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['f1arl', 'm2']
) as dag:

    load_data_task = PythonOperator(
        task_id='load_text_to_mysql',
        python_callable=load_text_to_mysql
    )

    notify_task = PythonOperator(
        task_id='send_completion_email_task',
        python_callable=send_completion_email
    )

    load_data_task >> notify_task