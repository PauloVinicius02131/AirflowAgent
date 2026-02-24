# syntax=docker/dockerfile:1
FROM apache/airflow:3.1.7

USER root

# 1. Ajuste do Usuário (Sua config original)
RUN usermod -u 1001 airflow && gpasswd -a airflow root

# ------------------------------------------------------------
# 2. System deps + MS ODBC 17 + IBM iAccess Dependencies
# ------------------------------------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      curl gnupg2 ca-certificates apt-transport-https \
      unixodbc unixodbc-dev \
      odbcinst \
      gcc g++ \
      # Adicionamos o procps que ajuda o instalador da IBM a verificar processos
      procps \
    ; \
    # Remove qualquer repo anterior da Microsoft (limpeza preventiva)
    find /etc/apt/sources.list.d -maxdepth 1 -type f -name "*.list" -print0 \
      | xargs -0 -I {} sh -c 'grep -q "packages.microsoft.com" "{}" && rm -f "{}" || true'; \
    \
    # Adiciona chave e repo Microsoft (Debian 12 / bookworm)
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/microsoft-prod.list; \
    apt-get update; \
    # Instalação da Versão 17
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      msodbcsql17 \
      mssql-tools \
    ; \
    # Ajuste dos links simbólicos
    ln -sf /opt/mssql-tools/bin/bcp /usr/local/bin/bcp; \
    ln -sf /opt/mssql-tools/bin/sqlcmd /usr/local/bin/sqlcmd; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# 3. Instalação do Driver IBM DB2 (iSeries)
# ------------------------------------------------------------
# Usando o nome exato do arquivo que você confirmou ter na pasta
COPY ibm-iaccess-1.1.0.28-1.0.amd64.deb /tmp/ibm-iaccess.deb

RUN set -eux; \
    # Instalamos o .deb. O dpkg pode reclamar de dependências, o 'apt-get install -f' resolve se necessário.
    dpkg -i /tmp/ibm-iaccess.deb || apt-get install -f -y; \
    rm /tmp/ibm-iaccess.deb; \
    # Verificação do registro do driver
    odbcinst -q -d

# ----------------------------
# 4. Permissions (Mantendo seu padrão 1001:0)
# ----------------------------
RUN set -eux; \
    mkdir -p /opt/airflow/config /opt/airflow/auth /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins; \
    chown -R 1001:0 /opt/airflow /home/airflow; \
    chmod -R g+rwX /opt/airflow /home/airflow

USER 1001

# ----------------------------
# 5. Python deps com Constraints
# ----------------------------
ARG AIRFLOW_VERSION=3.1.7

RUN set -eux; \
    # Simplificamos o comando Python para evitar erro de caractere de escape
    PY_VER=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"); \
    echo "Using Airflow constraints: ${AIRFLOW_VERSION} / Python ${PY_VER}"; \
    curl -fsSL \
      "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VER}.txt" \
      -o /tmp/constraints.txt

COPY --chown=airflow:0 requirements.txt /requirements.txt

RUN set -eux; \
    pip install --no-cache-dir -r /requirements.txt --constraint /tmp/constraints.txt; \
    rm -f /tmp/constraints.txt