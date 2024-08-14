import datetime
import logging
from airflow.decorators import dag
from airflow.providers.http.operators.http import HttpOperator  
from airflow.operators.python import PythonOperator
#from airflow.hooks.base import BaseHook
from shared_functions import insert_update_player_data_bulk_2

def health_check_response(response):
    logging.info(f"Response status code: {response.status_code}")
    logging.info(f"Response body: {response.text}")
    return (
        response.status_code == 200 
        and response.json() == {"message": "API health check successful"}
    )

def insert_update_player_data(**context):
    
    player_json = context['ti'].xcom_pull(task_ids='api_player_query')
    
    if player_json:
        insert_update_player_data_bulk_2(player_json)

    else:
       logging.warning("No player data found.")


@dag(start_date=datetime.datetime(2024, 8, 7), schedule_interval=None, catchup=False)  
def recurring_player_api_insert_update_dag():
    api_health_check_task = HttpOperator( 
        task_id='check_api_health_check_endpoint',
        http_conn_id='sportsworldcentral_url',  
        endpoint='/', 
        method='GET',
        headers={"Content-Type": "application/json"},
        response_check=health_check_response,
    )

    api_player_query_task = HttpOperator(  
        task_id='api_player_query',
        http_conn_id='sportsworldcentral_url',  
        endpoint='/v0/players/?skip=0&limit=100000&minimum_last_changed_date=2024-04-01', 
    #    endpoint='/v0/players/?skip=0&limit=100000&minimum_last_changed_date={{ (data_interval_start - macros.timedelta(days=1)) | ds }}',
        method='GET',
        headers={"Content-Type": "application/json"},
    )


    player_sqlite_upsert_task = PythonOperator(
        task_id='player_sqlite_upsert',
        python_callable=insert_update_player_data,
        provide_context=True,
    )

    # Define the task dependencies
    api_health_check_task >> api_player_query_task >> player_sqlite_upsert_task

# Instantiate the DAG
dag_instance = recurring_player_api_insert_update_dag()