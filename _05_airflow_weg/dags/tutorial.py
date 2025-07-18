from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json

def _captura_conta_dados(ti):
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    ti.xcom_push(key='qtd', value=qtd)

def _e_valida(ti):
    qtd = ti.xcom_pull(task_ids='captura_conta_dados', key='qtd')
    print(f'Quantidade de dados capturados: {qtd}')
    if qtd > 1000:
        return 'valido'
    else:
        return 'nvalido'

with DAG(
    'tutorial_dag_01',
    start_date=datetime(2023, 6, 1),
    schedule_interval='30 * * * *',
    catchup=False
) as dag:

    captura = PythonOperator(
        task_id='captura_conta_dados',
        python_callable=_captura_conta_dados
    )

    valida = BranchPythonOperator(
        task_id='e_valida',
        python_callable=_e_valida
    )

    valido = BashOperator(
        task_id='valido',
        bash_command='echo "Dados válidos"'
    )

    nvalido = BashOperator(
        task_id='nvalido',
        bash_command='echo "Quantidade não OK"'
    )

    # Encadeamento
    captura >> valida >> [valido, nvalido]

