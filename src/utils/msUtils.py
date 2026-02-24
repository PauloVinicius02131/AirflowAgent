# src/utils/msUtils.py

import requests
from msal import PublicClientApplication
from utils.customLogging import get_logger
from utils.qualityAssurance import get_config
import inspect
import json
import time
from datetime import datetime

# Base do Power BI REST API
token_scope = ["https://analysis.windows.net/powerbi/api/.default"]
BASE_URL = "https://api.powerbi.com/v1.0/myorg"
TIMEOUT = 30


class PowerBI:
    def __init__(self):
        """
        Construtor da classe PowerBI:
        - Obtém configuração do MSAL via get_config()
        - Instancia PublicClientApplication
        - Configura um logger para toda a classe
        """
        cfg = get_config()["Microsoft"]
        authority = f"https://login.microsoftonline.com/{cfg['TENANT_ID']}"
        self.app = PublicClientApplication(cfg["APP_ID"], authority=authority)
        self.username = cfg["USERNAME"]
        self.password = cfg["PASSWORD"]

        # stack = inspect.stack()
        # for idx, frame in enumerate(stack):
        #     print(f"Frame {idx}: function={frame.function}, file={frame.filename}, line={frame.lineno}")

        # Logger do módulo (nome do módulo + método chamarão via self.logger)
        caller_frame = inspect.stack()[2]
        caller_module = inspect.getmodule(caller_frame[0])
        job_name = getattr(caller_module, "__name__", "unknown_job")
        self.logger = get_logger(job_name, caller_frame.function)

    def get_access_token(self):
        """
        Autentica via MSAL e retorna o token de acesso para chamadas à API.
        """
        result = self.app.acquire_token_by_username_password(
            username=self.username,
            password=self.password,
            scopes=token_scope
        )
        token = result.get("access_token")
        if not token:
            error = result.get("error_description", "erro desconhecido")
            self.logger.error(f"Falha ao obter token: {error}")
            raise RuntimeError(f"Falha ao obter token: {error}")
        self.logger.debug("Access token obtido com sucesso")
        return token

    def list_workspaces(self, token):
        """
        Lista workspaces (grupos) do Power BI.
        """
        url = f"{BASE_URL}/groups"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        workspaces = resp.json().get("value", [])
        self.logger.debug(f"{len(workspaces)} workspaces retornados")
        return workspaces

    def get_group_id(self, token, workspace_name):
        """
        Retorna o ID de um workspace (grupo) pelo nome.
        """
        for ws in self.list_workspaces(token):
            if ws.get("name") == workspace_name:
                self.logger.debug(f"Group ID encontrado: {ws['id']}")
                return ws["id"]
        self.logger.error(f"Workspace '{workspace_name}' não encontrado")
        raise ValueError(f"Workspace '{workspace_name}' não encontrado")

    def list_dataflows(self, token, group_id):
        """
        Lista os dataflows de um grupo.
        """
        url = f"{BASE_URL}/groups/{group_id}/dataflows"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        dataflows = resp.json().get("value", [])
        self.logger.debug(f"{len(dataflows)} dataflows em grupo {group_id}")
        return dataflows

    def get_dataflow_id(self, token, group_id, dataflow_name):
        """
        Retorna o ID de um dataflow pelo nome.
        """
        for df in self.list_dataflows(token, group_id):
            if df.get("name") == dataflow_name:
                self.logger.debug(f"Dataflow ID encontrado: {df['objectId']}")
                return df["objectId"]
        self.logger.error(f"Dataflow '{dataflow_name}' não encontrado")
        raise ValueError(f"Dataflow '{dataflow_name}' não encontrado")

    def list_datasets(self, token, group_id):
        """
        Lista os datasets de um grupo.
        """
        url = f"{BASE_URL}/groups/{group_id}/datasets"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        datasets = resp.json().get("value", [])
        self.logger.debug(f"{len(datasets)} datasets em grupo {group_id}")
        return datasets

    def get_dataset_id(self, token, group_id, dataset_name):
        """
        Retorna o ID de um dataset pelo nome.
        """
        for ds in self.list_datasets(token, group_id):
            if ds.get("name") == dataset_name:
                self.logger.debug(f"Dataset ID encontrado: {ds['id']}")
                return ds["id"]
        self.logger.error(f"Dataset '{dataset_name}' não encontrado")
        raise ValueError(f"Dataset '{dataset_name}' não encontrado")

    def refresh_dataset(self, token, group_id, dataset_id):
        """
        Inicia o refresh de um dataset inteiro.
        """
        url = f"{BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes"
        headers = {"Authorization": f"Bearer {token}"}
        payload = {"notifyOption": "MailOnFailure"}

        resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
        if resp.status_code == 400:
            self.logger.error(f"Erro ao iniciar refresh: HTTP {resp.status_code} - {resp.text}")
            raise RuntimeError(f"Erro ao iniciar refresh: {resp.text}")
        resp.raise_for_status()

        request_id = resp.headers.get("requestid") or resp.headers.get("RequestId")

        self.logger.info(f"Refresh iniciado (HTTP {resp.status_code}), requestId={request_id}")
        return request_id

    def refresh_dataset_table(self, token, group_id, dataset_id, table_name):
        """
        Inicia o refresh de uma tabela específica em um dataset.
        """
        url = f"{BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "full",
            "objects": [{"table": table_name}]
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
        resp.raise_for_status()
        request_id = resp.headers.get("requestid") or resp.headers.get("RequestId")
        self.logger.info(f"Refresh de tabela '{table_name}' iniciado, requestId={request_id}")
        return request_id

    def refresh_dataset_table_incremental(self, token, group_id, dataset_id, table_name):
        """
        Inicia o refresh incremental de uma tabela específica.
        Requer política de Incremental Refresh configurada no Desktop.
        """
        url = f"{BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "automatic",            
            "commitMode": "transactional",  
            "applyRefreshPolicy": True,     
            "objects": [{"table": table_name}],
            "notifyOption": "MailOnFailure"
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
        resp.raise_for_status()

        request_id = resp.headers.get("requestid") or resp.headers.get("RequestId")
        self.logger.info(
            f"Refresh incremental da tabela '{table_name}' iniciado, requestId={request_id}"
        )
        return request_id

    def refresh_dataflow_table(self, token, group_id, dataflow_id, table_name="F_Faturamento"):
        """
        Inicia o refresh de uma tabela específica em um dataflow.
        """
        url = f"{BASE_URL}/groups/{group_id}/dataflows/{dataflow_id}/refreshes"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "full",
            "objects": [{"table": table_name}]
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
        resp.raise_for_status()
        request_id = resp.headers.get("requestid") or resp.headers.get("RequestId")
        self.logger.info(f"Refresh de dataflow '{table_name}' iniciado, requestId={request_id}")
        return request_id

    def get_refresh_status(self, token, group_id, dataset_id):
        """
        Retorna o status do último refresh de um dataset, aguardando até completar ou falhar.
        """
        url = f"{BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes"
        headers = {"Authorization": f"Bearer {token}"}

        while True:
            resp = requests.get(url, headers=headers, timeout=TIMEOUT)
            resp.raise_for_status()
            statuses = resp.json().get("value", [])
            if not statuses:
                self.logger.error(f"Nenhum refresh encontrado para dataset {dataset_id}")
                return {}

            latest = statuses[0]
            status = latest.get("status")
            refresh_type = latest.get("refreshType")
            request_id = latest.get("requestId")
            start_time = latest.get("startTime")
            end_time = latest.get("endTime")
            attempts = latest.get("refreshAttempts", [])

            if status == "Failed":
                error_msg = "erro desconhecido"
                service_exception = latest.get("serviceExceptionJson")
                if service_exception:
                    try:
                        error_json = json.loads(service_exception)
                        error_msg = error_json.get("errorCode", error_msg)
                    except Exception:
                        error_msg = service_exception
                self.logger.error(
                    f"Refresh falhou! Status: {status} Tipo: {refresh_type} "
                    f"RequestId: {request_id} Início: {start_time} Fim: {end_time} "
                    f"Erro: {error_msg} Tentativas: {attempts}"
                )
                raise RuntimeError(f"Refresh falhou: {error_msg}")

            if status == "Completed":
                self.logger.info(
                    f"Refresh concluído! Status: {status} Tipo: {refresh_type} "
                    f"RequestId: {request_id} Início: {start_time} Fim: {end_time} "
                    f"Tentativas: {attempts}"
                )
                return latest

            # Se ainda em andamento, aguarda antes de checar novamente
            time.sleep(30)
