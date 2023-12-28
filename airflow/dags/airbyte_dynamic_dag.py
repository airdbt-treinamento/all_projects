from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

import json
import pandas as pd


default_args = {
    'owner': 'irineugomes',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout' : timedelta(minutes=5)
}


json_data = """
    [
        {"connectionId": "40a30a4a-f6c5-4c18-af43-60a185cb8889", "name": "Postgres_to_BigQuery__sales_customer"},
        {"connectionId": "4db44db0-2bca-41c3-ba05-d1451680d361", "name": "Postgres_to_BigQuery__sales_salesreason"},
        {"connectionId": "6099f32a-f661-4c56-b0cb-b2ae3ce38c6b", "name": "Postgres_to_BigQuery__production_product"},
        {"connectionId": "735716c0-cc3e-4034-a577-354e524e6e37", "name": "Postgres_to_BigQuery__person_address"},
        {"connectionId": "7bec2156-3590-4e8f-89af-0459fc1ac04e", "name": "Postgres_to_BigQuery__person_stateprovince"},
        {"connectionId": "801730b5-0cf3-459b-a710-9863fd3d096a", "name": "Postgres_to_BigQuery__person_countryregion"},
        {"connectionId": "845df753-bd55-41bf-a6b6-8956e8e3adaa", "name": "Postgres_to_BigQuery__person_person"},
        {"connectionId": "8ca07fbc-7691-4a1a-89d7-f82ab09b4335", "name": "Postgres_to_BigQuery__sales_creditcard"},
        {"connectionId": "a12f51e1-ea92-44dd-baaa-0195abbb41d0", "name": "Postgres_to_BigQuery__sales_store"},
        {"connectionId": "c51aa443-79e6-42d6-9ebb-ba4df07aa63f", "name": "Postgres_to_BigQuery__sales_salesorderdetail"},
        {"connectionId": "d3c3bff3-f9e3-487e-a6bc-6d15f4b4c4cf", "name": "Postgres_to_BigQuery__sales_salesorderheadersalesreason"},
        {"connectionId": "e7d46fb5-57e0-4525-a8e7-6fb568170cc4", "name": "Postgres_to_BigQuery__sales_salesorderheader"}
    ]
"""

json01 = json.dumps(json.loads(json_data))
#print(json01)
#print(len(json01))

airbyte_list = pd.read_json(json01)
#print(df)

PARAMS_FILE = airbyte_list

with DAG('_Airbyte_dag_dynamic_parallel',
         description = "DAG dynamic with Airbyte",
         start_date        = days_ago(1),
         max_active_runs   = 1,
         catchup           = False,
         schedule_interval = None,
         default_args      = default_args,
         tags              = ["Airbyte"],
    ) as dag:

    start = DummyOperator(task_id='Inicio')

    end = DummyOperator(task_id='Fim')

    with TaskGroup("airbyte") as ab:
        for index, row in PARAMS_FILE.iterrows():
            t = AirbyteTriggerSyncOperator(
                task_id=row[1],
                airbyte_conn_id='airbyte_conn_id',
                connection_id=row[0],
                asynchronous=False,
                timeout=3600,
                wait_seconds=3,
                pool='pool_airbyte'
            )

    start >> ab >> end
