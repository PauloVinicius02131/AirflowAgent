-- Garante que o schema existe (caso não tenha rodado ainda)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'connect')
BEGIN
    EXEC('CREATE SCHEMA connect');
END
GO

-- Criação da tabela de veículos
IF OBJECT_ID('connect.veiculos_frota', 'U') IS NOT NULL
    DROP TABLE connect.veiculos_frota;
GO

CREATE TABLE connect.veiculos_frota (
    -- Chave Natural / Identificador Único
    vin VARCHAR(250) NOT NULL,
    
    -- Informações Gerais
    customerVehicleName VARCHAR(250),
    brand VARCHAR(250),
    
    -- Datas tratadas no Pandas (Sugerido converter yyyy-mm-dd)
    productionDate VARCHAR(250),
    deliveryDate VARCHAR(250),
    
    -- Especificações Técnicas
    vehicleType VARCHAR(250), -- Renomeado de 'type' (palavra reservada)
    model VARCHAR(250),
    emissionLevel VARCHAR(250),
    
    -- Informações de Operação (volvoGroupVehicle)
    countryOfOperation VARCHAR(250),
    registrationNumber VARCHAR(250),
    roadCondition VARCHAR(250),
    transportCycle VARCHAR(250),
    roadOverspeedLimit VARCHAR(250),
    
    -- Auditoria (Preenchimento Automático)
    data_gravacao DATETIME2(3) NOT NULL 
        CONSTRAINT DF_veiculos_frota_data_gravacao DEFAULT (GETDATE()),

    -- Restrições
    CONSTRAINT PK_veiculos_frota PRIMARY KEY NONCLUSTERED (vin)
);

-- Otimização para Analytics: Clustered Columnstore Index
-- Ideal para compressão e performance em consultas de agregação/BI
CREATE CLUSTERED COLUMNSTORE INDEX CCI_veiculos_frota 
ON connect.veiculos_frota;
GO