# include/dbt/cosmos_config.py

from cosmos.config import ProfileConfig, ProjectConfig
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='dbt_live_projeto',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt_live_projeto/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt_live_projeto/',
)
