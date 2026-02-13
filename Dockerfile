# syntax=docker/dockerfile:1
FROM apache/airflow:3.1.7

USER root

# 1. Ajuste do Usuário: Mudamos o UID do airflow para 1001. 
# Não tentamos modificar o grupo, apenas garantimos que o usuário 1001 exista e 
# pertença ao grupo 0 (root), que já tem as permissões necessárias.
RUN usermod -u 1001 airflow && gpasswd -a airflow root


# ----------------------------
# System deps + MS ODBC + tools (bcp/sqlcmd)
# ----------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      curl gnupg2 ca-certificates apt-transport-https \
      unixodbc unixodbc-dev \
      gcc g++ \
    ; \
    # Remove qualquer repo anterior da Microsoft (evita conflito Signed-By)
    find /etc/apt/sources.list.d -maxdepth 1 -type f -name "*.list" -print0 \
      | xargs -0 -I {} sh -c 'grep -q "packages.microsoft.com" "{}" && rm -f "{}" || true'; \
    \
    # Adiciona chave e repo Microsoft (Debian 12 / bookworm)
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/microsoft-prod.list; \
    apt-get update; \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      msodbcsql18 \
      mssql-tools18 \
    ; \
    ln -sf /opt/mssql-tools18/bin/bcp /usr/local/bin/bcp; \
    ln -sf /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/sqlcmd; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# ----------------------------
# Permissions (Ajustadas para o UID 1001 e Grupo Root)
# ----------------------------
RUN set -eux; \
    mkdir -p /opt/airflow/config /opt/airflow/auth /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins; \
    # Mudamos a posse para o UID 1001 e GID 0
    chown -R 1001:0 /opt/airflow /home/airflow; \
    chmod -R g+rwX /opt/airflow /home/airflow

# Voltamos para o usuário 1001 (o seu 'paulo' do WSL)
USER 1001

# ----------------------------
# IMPORTANT: Install Python deps with Airflow constraints
# This prevents provider/core mismatch (your current crash)
# ----------------------------
ARG AIRFLOW_VERSION=3.1.7

# Descobre a versão exata do Python da imagem (ex.: 3.12) e baixa a constraints correspondente
RUN set -eux; \
    PY_VER="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"; \
    echo "Using Airflow constraints: ${AIRFLOW_VERSION} / Python ${PY_VER}"; \
    curl -fsSL \
      "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VER}.txt" \
      -o /tmp/constraints.txt

COPY --chown=airflow:0 requirements.txt /requirements.txt

# Instala requirements TRAVADO nas constraints do Airflow (evita quebrar celery provider)
RUN set -eux; \
    pip install --no-cache-dir -r /requirements.txt --constraint /tmp/constraints.txt; \
    rm -f /tmp/constraints.txt

# (Opcional) sanity check rápido: mostra versões no build log
RUN set -eux; \
    python -c "import airflow; print('Airflow:', airflow.__version__)"; \
    pip show apache-airflow-providers-celery >/dev/null 2>&1 && pip show apache-airflow-providers-celery | sed -n '1,80p' || true
