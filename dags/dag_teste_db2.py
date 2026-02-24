from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import sys
import os

# Garante que o diretório SRC está no path para as tasks encontrarem seu DAO
sys.path.append('/opt/airflow/src')

default_args = {
    'owner': 'Paulo',
    'start_date': datetime(2026, 2, 1),
}

with DAG(
    'test_db2_connection_v1',
    default_args=default_args,
    schedule=None,  # Disparo apenas manual
    catchup=False,
    tags=['test', 'db2', 'infra'],
) as dag:

    @task
    def check_system_drivers():
        """Verifica se o sistema operacional reconhece o driver instalado"""
        import subprocess
        print("--- Verificando drivers ODBC instalados no OS ---")
        result = subprocess.run(['odbcinst', '-q', '-d'], capture_output=True, text=True)
        print(result.stdout)
        
        if 'IBM i Access ODBC Driver' in result.stdout:
            print("✅ Driver IBM i Access encontrado!")
        else:
            print("❌ Driver IBM i Access NÃO encontrado nos drivers do sistema.")
        return result.stdout

    @task
    def test_db2_query():
        """Tenta uma conexão real e um select básico via sua classe DAO"""
        # Import interno para evitar erro de importação no scheduler se o path falhar
        from DAO.DataAcessObject import BR_DB2
        import pandas as pd
        
        print("--- Iniciando conexão com DB2 via DAO ---")
        try:
            db = BR_DB2()
            # Select básico no iSeries/AS400 para testar vida
            query = "SELECT 1 FROM sysibm.sysdummy1"
            
            df = db.select(query)
            
            print("✅ Conexão bem sucedida!")
            print(f"Retorno do Banco: \n{df.to_string()}")
            return True
        except Exception as e:
            print(f"❌ Falha na conexão: {str(e)}")
            # Printa o sys.path para debug caso não encontre o módulo
            print(f"Debug PATH: {sys.path}")
            raise e

    # Fluxo
    check_system_drivers() >> test_db2_query()