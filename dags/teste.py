import os
import pyodbc
import pwd
import subprocess
import pandas as pd
import socket
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime



def check_infra():
    print("=== 1. IDENTIDADE E PERMISSÕES ===")
    uid = os.getuid()
    print(f"✅ UID: {uid} | Usuário: {pwd.getpwuid(uid).pw_name}")
    
    print("\n=== 2. DRIVERS E FERRAMENTAS SQL ===")
    drivers = [driver for driver in pyodbc.drivers()]
    print(f"✅ Drivers ODBC: {drivers}")
    
    # Testando se o BCP (Bulk Copy) está no PATH
    try:
        bcp_version = subprocess.check_output(["bcp", "-v"]).decode().strip()
        print(f"✅ BCP Tool instalada: {bcp_version}")
    except Exception:
        print("❌ ERRO: BCP não encontrado no PATH.")

    print("\n=== 3. PROCESSAMENTO DE DADOS (PANDAS) ===")
    try:
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        # Tenta salvar um parquet na pasta de logs (teste de escrita + biblioteca)
        test_path = '/opt/airflow/logs/test_data.parquet'
        df.to_parquet(test_path)
        print(f"✅ Pandas + PyArrow: DataFrame criado e salvo em Parquet.")
        os.remove(test_path)
    except Exception as e:
        print(f"❌ ERRO no processamento de dados: {e}")

    print("\n=== 4. CONECTIVIDADE DE REDE ===")
    try:
        # Teste de DNS e Internet
        host = "google.com"
        ip = socket.gethostbyname(host)
        print(f"✅ Rede: DNS funcionando. {host} resolve para {ip}")
    except Exception as e:
        print(f"❌ ERRO de Rede: Container sem acesso externo ou DNS falhou.")

    print("\n=== 5. AIRFLOW CONFIGS ===")
    executor = os.environ.get('AIRFLOW__CORE__EXECUTOR')
    print(f"✅ Executor atual: {executor}")

with DAG(
    dag_id='00_sanity_check_ambiente',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['infra', 'teste'],
) as dag:

    t1 = PythonOperator(
        task_id='validar_drivers_e_permissoes',
        python_callable=check_infra
    )