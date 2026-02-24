import pandas as pd
from datetime import datetime

class VolvoVehiclesTransformer:
    @staticmethod
    def transform(raw_data):
        if not raw_data:
            return pd.DataFrame()
        
        # 1. Achata o JSON
        df = pd.json_normalize(raw_data)
        
        # 2. Tratamento da Data de Produção
        date_cols = {'productionDate.year': 'year', 'productionDate.month': 'month', 'productionDate.day': 'day'}
        if all(col in df.columns for col in date_cols):
            # Criamos com o nome EXATO da tabela: productionDate
            df['productionDate'] = pd.to_datetime(df[list(date_cols.keys())].rename(columns=date_cols), errors='coerce')
        else:
            df['productionDate'] = None

        # 3. Mapeamento seguindo a ORDEM EXATA do seu SELECT no SQL
        # Chave: Nome no JSON achatado | Valor: Nome na Tabela SQL
        rename_map = {
            'vin': 'vin',
            'customerVehicleName': 'customerVehicleName',
            'brand': 'brand',
            'productionDate': 'productionDate', # A coluna que tratamos acima
            'volvoGroupVehicle.deliveryDate': 'deliveryDate',
            'type': 'vehicleType',
            'model': 'model',
            'emissionLevel': 'emissionLevel',
            'volvoGroupVehicle.countryOfOperation': 'countryOfOperation',
            'volvoGroupVehicle.registrationNumber': 'registrationNumber',
            'volvoGroupVehicle.roadCondition': 'roadCondition',
            'volvoGroupVehicle.transportCycle': 'transportCycle',
            'volvoGroupVehicle.vehicleReportSettings.roadOverspeedLimit': 'roadOverspeedLimit'
        }

        # 4. Filtra e Renomeia na ordem correta
        # Nota: Não incluímos 'data_gravacao' aqui porque o seu DAO/BCP 
        # deve ignorar essa coluna ou ela deve ser preenchida pelo SQL.
        df_final = df[list(rename_map.keys())].rename(columns=rename_map)
        
        # Garantir que as colunas de data sejam strings formatadas para o SQL não se perder no BCP
        for col in ['productionDate', 'deliveryDate']:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

        return df_final