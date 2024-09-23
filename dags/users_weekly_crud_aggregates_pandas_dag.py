import os.path
from datetime import datetime
import sys
from os.path import dirname, realpath

from airflow.decorators import dag, task

begin_path = dirname(dirname(realpath(__file__)))
sys.path.append(begin_path)

from dags.utils.users_crud_aggregates_pandas import calculate_daily_aggregates, calculate_weekly_aggregates

default_args = {
    "owner": "VK",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 11),
    # "email": [""],
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "retries": 5,
    # "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


@dag(
    "users_weekly_crud_aggregates_pandas_dag",
    default_args=default_args,
    schedule_interval='0 7 * * *',
    # schedule_interval='* * * */1 *',  # для тестов
    catchup=False,
)
def users_weekly_crud_aggregates_taskflow():
    input_path = os.path.join(begin_path, 'dags', 'input')
    output_path = os.path.join(begin_path, 'dags', 'output')

    @task()
    def daily_aggregates_task():
        calculate_daily_aggregates(datetime.now().date(), input_path, 7)
        return True

    @task()
    def weekly_aggregates_task():
        calculate_weekly_aggregates(datetime.now().date(), input_path, output_path, 7)
        return True

    daily_aggregates_task() >> weekly_aggregates_task()

users_weekly_crud_aggregates_taskflow()
