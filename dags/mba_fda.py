from __future__ import annotations
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import requests
from datetime import date

# Configurações
GCP_PROJECT  = "bigquery-sandbox-471123"
BQ_DATASET   = "dataset_fda"
BQ_TABLE     = "openfda_sildenafil_range_test"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"
USE_POOL     = True
POOL_NAME    = "openfda_api"
TEST_START = date(2025, 6, 1)
TEST_END   = date(2025, 7, 29)
DRUG_QUERY = 'sildenafil+citrate'

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: voce@exemplo.gov.br)"})

def _openfda_get(url: str) -> dict:
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()

def _build_openfda_url(start: date, end: date, drug_query: str) -> str:
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    return ("https://api.fda.gov/drug/event.json"
            f"?search=patient.drug.medicinalproduct:%22{drug_query}%22"
            f"+AND+receivedate:[{start_str}+TO+{end_str}]"
            "&count=receivedate")

_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME

@task(**_task_kwargs)
def fetch_fixed_range_and_to_bq():
    url = _build_openfda_url(TEST_START, TEST_END, DRUG_QUERY)
    data = _openfda_get(url)
    results = data.get("results", [])
    if not results:
        return
    df = pd.DataFrame(results).rename(columns={"count": "events"})
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)
    df["drug"]      = DRUG_QUERY.replace("+", " ")
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    df.to_gbq(destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
              project_id=GCP_PROJECT,
              if_exists="append",
              credentials=bq_hook.get_credentials(),
              table_schema=[
                {"name": "time", "type": "TIMESTAMP"},
                {"name": "events", "type": "INTEGER"},
                {"name": "win_start", "type": "DATE"},
                {"name": "win_end", "type": "DATE"},
                {"name": "drug", "type": "STRING"}],
              location=BQ_LOCATION,
              progress_bar=False)

@dag(dag_id="openfda_mcb2_test_range",
     schedule="@once",
     start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
     catchup=False,
     max_active_runs=1,
     tags=["openfda", "bigquery", "test", "range"])
def openfda_pipeline_test_range():
    fetch_fixed_range_and_to_bq()

dag = openfda_pipeline_test_range()
