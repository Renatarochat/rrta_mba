FROM astrocrpublic.azurecr.io/runtime:3.0-11-python-3.12

# 2. Switch to the 'root' user to get permissions for installation
USER root

# 3. Now, run commands to install system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential

# 4. Switch back to the default, non-root 'astro' user for security
USER astro
#ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
#ENV AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE=True
#ENV AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
#ENV AIRFLOW__CORE__DAGS_FOLDER=~/airflow/dags
#RUN apt-get update && apt-get install -y build-essential gcc python3-dev
RUN python -m pip install --upgrade pip
