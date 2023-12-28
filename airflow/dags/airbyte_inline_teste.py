from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.task_group import TaskGroup

import json
import pandas as pd
from datetime import datetime, timedelta

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
            {"connectionId": "2fc18b46-ed7e-4d5c-b01e-2afc6ff53ccc", "name": "Postgres_to_BigQuery__sales_customer"}
        ]
    """
json01 = json.dumps(json.loads(json_data))
df = pd.read_json(json01)

with DAG('_Airbyte_inline_sequence_teste',
         description = "DAG com Airbyte executando em linha",
         start_date        = datetime(2023,10,1),
         max_active_runs   = 1,
         catchup           = False,
         schedule_interval = None,
         default_args      = default_args,
         tags              = ["Airbyte"],
    ) as dag:

    start = DummyOperator(task_id='Inicio')

    end = DummyOperator(task_id='Fim')

    with TaskGroup("Airbyte") as ab:
        list = []
        for i in range(len(df)):
            list.append(AirbyteTriggerSyncOperator(
                task_id         = df.iloc[i,1],
                airbyte_conn_id = 'airbyte_conn_id',
                connection_id   = df.iloc[i,0],
                asynchronous    = False,
                timeout         = 3600,
                wait_seconds    = 3
                ))
            if len(list) > 1:
                list[-2] >> list[-1]

    start >> ab >> end
