from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import json
import pandas as pd

default_args = {
    'owner': 'Paulo',
    'start_date': datetime(2026, 2, 1),
    'retries': 2,
    'retry_delay': 300 
}

with DAG(
    'etl_volvo_connect_vehicles_v2',
    default_args=default_args,
    catchup=False,
    tags=['volvo', 'data_engineer', 'sql_server_tuning'],
) as dag:

    @task
    def get_clients_config():
        path = "/opt/airflow/auth/volvo_connect_credentials.json"
        with open(path, 'r') as f:
            configs = json.load(f)
        return [c for c in configs if c.get('enabled')]

    @task
    def extract_task(config):
        from Extract.VolvoConnect.Vehicles import VolvoVehiclesExtractor
        
        client_key = config['client_key']
        creds = {client_key: {"username": config['username'], "password": config['password']}}
        
        extractor = VolvoVehiclesExtractor(client_key, creds)
        raw_data = extractor.extract_all()
        
        return {"client_key": client_key, "raw_data": raw_data}

    @task
    def transform_and_load_task(extraction_result):
        from Transform.VolvoConnect.VehiclesTransform import VolvoVehiclesTransformer
        from DAO.DataAcessObject import DW_Treviso_Tuning
        
        client_key = extraction_result['client_key']
        raw_data = extraction_result['raw_data']
        
        if not raw_data:
            print(f"Sem dados para o cliente {client_key}")
            return
        
        # 1. Transformação (Pandas)
        transformer = VolvoVehiclesTransformer()
        df = transformer.transform(raw_data)
        
        # 2. Carga Otimizada (DAO)
        # Instancia seu DAO especializado
        dao = DW_Treviso_Tuning()
        
        # Realiza o Upsert (Merge)
        # O DAO automaticamente identifica a PK (VIN) via introspecção
        # 'DT_GRAVACAO' é ignorado no BCP e gerido pelo SQL Server via Default/Merge
        resultado = dao.sync(
            df=df,
            schema='connect',
            table='veiculos_frota',
            mode='upsert',
            audit_dir='/opt/airflow/logs/audit/volvo/',
            audit='all' # Ativa log de Inserts/Updates/Deletes em JSONL
        )
        
        print(f"Sync concluído para {client_key}: {resultado}")
        return {client_key: resultado}

    # --- Pipeline Flow ---
    configs = get_clients_config()
    
    # Extração em paralelo para todos os clientes
    extracted_data = extract_task.expand(config=configs)
    
    # Transformação e Carga em paralelo conforme as extrações terminam
    transform_and_load_task.expand(extraction_result=extracted_data)