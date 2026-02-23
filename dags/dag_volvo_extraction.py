from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable # Para configurações não sensíveis
from datetime import datetime
import json

# Sugestão: Use Connections para credenciais reais
# Mas seguindo sua lógica de JSON para múltiplos clientes:

default_args = {
    'owner': 'Paulo',
    'start_date': datetime(2026, 2, 1),
    'retries': 2,
    'retry_delay': 300 # 5 min entre tentativas (bom para APIs)
}

with DAG(
    'etl_volvo_connect_vehicles',
    default_args=default_args,
    catchup=False,
    tags=['extract', 'volvo', 'data_eng']
    
) as dag:

    @task
    def get_clients_config():
        """
        Lê a configuração. Idealmente, isso viria de uma Variable 
        ou de um banco de metadados de controle.
        """
        path = "/opt/airflow/auth/volvo_connect_credentials.json"
        with open(path, 'r') as f:
            configs = json.load(f)
        # Retorna apenas os habilitados para o mapeamento dinâmico
        return [c for c in configs if c.get('enabled')]

    @task
    def run_extraction(config):
        """
        Esta task será replicada dinamicamente para cada cliente.
        """
        from Extract.VolvoConnect.Vehicles import VolvoVehiclesExtractor
        
        client_key = config['client_key']
        print(f"Iniciando extração: {client_key}")
        
        creds = config['username'], config['password']
        
        # Exemplo de instância do seu extrator
        extractor = VolvoVehiclesExtractor(client_key, {client_key: {"username": creds[0], "password": creds[1]}})
        data = extractor.extract_all()
        
        path = f"/opt/airflow/auth/{client_key}_vehicles.json"
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        
        return {"client": client_key, "status": "success"}

    # Fluxo de Execução
    configs = get_clients_config()
    
    # Dynamic Task Mapping: cria uma task 'run_extraction' para cada item em 'configs'
    extraction_results = run_extraction.override(
        map_index_template="{{ task.op_kwargs['config']['client_key'] }}"
    ).expand(config=configs)