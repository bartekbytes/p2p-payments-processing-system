from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timezone, timedelta

# Configuration for DAG
default_args = {
    "owner": "first_circle",
    "depends_on_past": False,
    "retries": 3, # for restartability purpose
    "retry_delay": timedelta(minutes=2),
}

def loading_start_registration():
    # Here should be a proper logic of registering a new loading
    # with metadata included (start time, load_id, etc...)
    print(f"Start of the loading: {datetime.now(timezone.utc).isoformat()}")
    
def loading_end_registration():
    # Here should be a proper logic of finalizing a new loading
    # with metadata included (end time, outcome, etc...)
    print(f"End of the loading: {datetime.now(timezone.utc).isoformat()}")

with DAG(
    dag_id="transactions_loading",
    default_args=default_args,
    description="DWH-DM Transactions Loading",
    start_date=datetime(2023, 1, 1),
    #schedule='*/15 * * * *', # if commented out, then run only on-demand mode
    catchup=False,
    tags=["pps", "transaction"],
) as dag:

    start_loading = PythonOperator(
        task_id='start_loading',
        python_callable=loading_start_registration
    )

    end_loading = PythonOperator(
        task_id='end_loading',
        trigger_rule=TriggerRule.ALL_DONE,
        python_callable=loading_end_registration
    )

    dbt_run_dwh = BashOperator(
        task_id="dbt_run_dwh",
        bash_command="docker exec pps_dbt dbt run --selector dwh --target prod",
    )

    dbt_test_dwh = BashOperator(
        task_id="dbt_test_dwh",
        bash_command="docker exec pps_dbt dbt test --selector dwh --target prod",
    )

    dbt_run_dm = BashOperator(
        task_id="dbt_run_dm",
        bash_command="docker exec pps_dbt dbt run --selector dm --target prod",
    )

    dbt_test_dm = BashOperator(
        task_id="dbt_test_dm",
        bash_command="docker exec pps_dbt dbt test --selector dm --target prod",
    )

    # Task dependencies
    start_loading >> dbt_run_dwh >> dbt_test_dwh >> dbt_run_dm >> dbt_test_dm >> end_loading
