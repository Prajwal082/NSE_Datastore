import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from project.NSE import Nse

def launch_bronze():

    nse = Nse()

    nse.get_DeliveryData()


dag = DAG(
    dag_id = "NSE_Orchestration",
    default_args = {
        "owner": "prajwal poojary",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)


bronze_job = PythonOperator(
    task_id="NSE_API_DATAPULL",
    python_callable = launch_bronze,
    dag=dag
)

bronze_job

