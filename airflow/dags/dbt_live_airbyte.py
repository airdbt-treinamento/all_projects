"""
### The Activity DAG

This DAG will help me decide what to do today. It uses the [BoredAPI](https://www.boredapi.com/) to do so.

Before I get to do the activity I will have to:

- Clean up the kitchen.
- Check on my pipelines.
- Water the plants.

Here are some happy plants:

<img src="https://www.publicdomainpictures.net/pictures/80000/velka/succulent-roses-echeveria.jpg" alt="plants" width="300"/>
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from cosmos import ExecutionConfig, RenderConfig, ProjectConfig, ProfileConfig
from cosmos.airflow.task_group import DbtTaskGroup
from pathlib import Path


default_args = {
    'owner': 'irineugomes',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout' : timedelta(minutes=5),
}


dbt_path = Path("/opt/airflow") / "dbt"
dwh_path = Path("/opt/airflow") / "dbt" / "dbt_live_projeto"
dbt_executable = Path("/home/airflow/.local/bin/dbt")

dbt_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)

dbt_profile_config = ProfileConfig(
    profile_name="dbt_live_projeto",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dbt/dbt_live_projeto/profiles.yml",
)

test_command = f"\
    dbt test \
    --profiles-dir {dbt_path} \
    --project-dir {dwh_path} \
    --target prod_airflow"


with DAG('_DBT-Core_Exemplo-01',
         description = "DAG com o DBT-Core",
         start_date        = datetime(2023,10,1),
         max_active_runs   = 1,
         catchup           = False,
         schedule_interval = None,
         default_args      = default_args,
         tags              = ["DBT"],
    ) as dag:

    dag.doc_md = __doc__

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    dbt_silver = DbtTaskGroup(
        group_id         = "Staging",
        project_config   = ProjectConfig(str(dwh_path)),
        profile_config   = dbt_profile_config,
        execution_config = dbt_execution_config,
        render_config    = RenderConfig(select=["path:models/silver"],),
        dag              = dag
    )

    dbt_gold = DbtTaskGroup(
        group_id         = "DW",
        project_config   = ProjectConfig(str(dwh_path)),
        profile_config   = dbt_profile_config,
        execution_config = dbt_execution_config,
        render_config    = RenderConfig(select=["path:models/gold"],),
        dag              = dag
    )

    run_dbt_tests_models = BashOperator(
        task_id='run_dbt_tests_models',
        bash_command=test_command
    )

    start >> dbt_silver >> dbt_gold >> run_dbt_tests_models >> end