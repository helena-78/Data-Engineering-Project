# Airflow 2.10.5 + dbt (separate venv) + clickhouse-connect + dbt packages baked in.
# Built by docker compose with the REPO ROOT as build context (see docker-compose.yml and .dockerignore);
# the same image runs the ad-hoc "dbt" service.
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

USER root
# git: required by `dbt debug` and by git-based dbt packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# dbt in its own virtualenv, exposed on PATH as `dbt` (Airflow's environment stays untouched).
COPY airflow/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt \
    && rm /tmp/requirements-dbt.txt

# dbt packages (dbt_utils, ...) are installed INSIDE the image, not in the bind-mounted ./dbt folder:
# on Docker Desktop for Windows `dbt deps` onto the bind mount can leave a half-extracted package while
# still exiting 0 (seen in the smoke test: dbt_utils without dbt_project.yml). dbt_project.yml reads
# packages-install-path from DBT_PACKAGES_INSTALL_PATH. After changing dbt/packages.yml, rebuild the
# image (docker compose build); the dbt_deps task also reinstalls when packages.yml differs from the baked copy.
# dbt deps deletes and recreates the install folder, so its parent /opt/dbt must be writable as well.
ENV DBT_PACKAGES_INSTALL_PATH=/opt/dbt/packages
COPY dbt/dbt_project.yml dbt/packages.yml dbt/package-lock.yml /tmp/dbt-deps/
RUN DBT_SEND_ANONYMOUS_USAGE_STATS=false /opt/dbt-venv/bin/dbt deps --project-dir /tmp/dbt-deps \
    && test -f /opt/dbt/packages/dbt_utils/dbt_project.yml \
    && cp /tmp/dbt-deps/packages.yml /opt/dbt/packages/.installed_packages.yml \
    && rm -rf /tmp/dbt-deps \
    && chown -R airflow:0 /opt/dbt \
    && chmod -R g+rwX /opt/dbt

# Extra packages for Airflow's own environment, pinned by the official constraints file.
USER airflow
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ENV DBT_BIN=/opt/dbt-venv/bin/dbt
