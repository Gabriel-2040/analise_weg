from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from pathlib import Path

# Importando funções do seu projeto
from funcoes.pipeline._1_converter_xls_csv import converter_xls_csv
from funcoes.pipeline._2_pivotar_csv import pivotar_csv
from funcoes.pipeline._3_no_acent_up_case import upper_noacent
from funcoes.pipeline._4_captura_conta_dados import captura_conta_dados


# Definindo pastas
pasta_origem = r'E:\PYTHON\PROJETOS_PESSOAIS\analise_weg\_02_DATASETS\datasets_original'
pasta_destino = r"E:\PYTHON\PROJETOS_PESSOAIS\analise_weg\_03_ANALISE_EXPLORATORIA\_03_02_analises_01"
pivotados = r'E:\PYTHON\PROJETOS_PESSOAIS\analise_weg\_04_ETL\_04_01_datasets_pivotados'
tratados = r"E:\PYTHON\PROJETOS_PESSOAIS\analise_weg\_04_ETL\_04_02_dataset_tratado"


converter_xls_csv(pasta_origem, pasta_destino)


base_dir = Path(r"E:\PYTHON\PROJETOS_PESSOAIS\analise_weg")
input_folder = base_dir / "_03_ANALISE_EXPLORATORIA\_03_02_analises_01"
output_folder = base_dir / "_04_ETL/_04_01_datasets_pivotados"
# Executar o processamento


with DAG(
    dag_id='pipeline_weg',
    start_date=datetime(2023, 6, 1),
    schedule_interval='30 * * * *',
    catchup=False,
    tags=['weg', 'etl']
) as dag:

    task_captura = PythonOperator(
        task_id='captura_conta_dados',
        python_callable=captura_conta_dados
    )

    task_converter = PythonOperator(
        task_id='converter_xls_csv',
        python_callable=converter_xls_csv,
        op_kwargs={'pasta_origem': pasta_origem, 'pasta_destino': pasta_destino}
    )

    task_pivotar = PythonOperator(
        task_id='pivotar_csv',
        python_callable=pivotar_csv,
        op_kwargs={'input_folder': pasta_destino, 'output_folder': pivotados}
    )

    task_upper = PythonOperator(
        task_id='remover_acentos_upper',
        python_callable=upper_noacent,
        op_kwargs={'input_folder': pivotados, 'output_folder': tratados}
    )

    # Encadeamento
    task_captura >> task_converter >> task_pivotar >> task_upper 