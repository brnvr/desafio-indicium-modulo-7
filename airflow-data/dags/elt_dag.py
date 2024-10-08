from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

from extract import extract_from_table
from load import load_into_csv, load_into_txt
from transform import count_items_ordered_to_rio

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

def process_export_orders():
    orders = extract_from_table("Order")
    load_into_csv("output_orders.csv", orders)

def process_count_items_to_rio():
    count = count_items_ordered_to_rio("output_orders.csv")
    load_into_txt("count.txt", str(count))


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

    export_orders = PythonOperator(
        task_id='extract_orders',
        python_callable = process_export_orders,
        provide_context=True
    )

    count_items_to_rio = PythonOperator(
        task_id='count_items_to_rio',
        python_callable = process_count_items_to_rio,
        provide_context=True
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    export_orders >> count_items_to_rio >> export_final_output
   
