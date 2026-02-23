import base64
import uuid
import http.client
import json
import logging
import os

class VolvoVehiclesExtractor:
    HOST = "api.volvotrucks.com"
    BASE_PATH = "/vehicle"
    
    ACCEPT_HEADERS = {
        "vehicles": "application/x.volvogroup.com.vehicles.v1.0+json",
        "vehicle_positions": "application/x.volvogroup.com.vehiclepositions.v1.0+json"
    }

    def __init__(self, cliente_key: str, credentials: dict):
        self.cliente_key = cliente_key
        self.cfg = credentials[cliente_key]
        self.token = self._build_token()

    def _build_token(self) -> str:
        raw = f"{self.cfg['username']}:{self.cfg['password']}".encode()
        return base64.b64encode(raw).decode()

    def extract_all(self):
        conn = http.client.HTTPSConnection(self.HOST)
        all_rows = []
        last_vin = None

        while True:
            request_id = str(uuid.uuid4())
            path = f"{self.BASE_PATH}/vehicles?additionalContent=VOLVOGROUPVEHICLE&requestId={request_id}"
            if last_vin:
                path += f"&lastVin={last_vin}"

            headers = {
                "Authorization": f"Basic {self.token}",
                "Accept": self.ACCEPT_HEADERS["vehicles"]
            }

            conn.request("GET", path, headers=headers)
            res = conn.getresponse()
            raw_data = res.read().decode("utf-8")

            if res.status >= 400:
                logging.error(f"Erro Cliente {self.cliente_key}: {res.status} - {raw_data}")
                break

            data = json.loads(raw_data)
            batch = data.get("vehicleResponse", {}).get("vehicles", [])
            all_rows.extend(batch)

            if not data.get("moreDataAvailable") or not batch:
                break

            last_vin = batch[-1]["vin"]
        
        conn.close()
        return all_rows