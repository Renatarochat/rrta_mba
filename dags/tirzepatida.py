# dags/openfda_tirzepatida_stage_pipeline.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pendulum
import pandas as pd
import pandas_gbq
import requests
from datetime import date, timedelta
from typing import Any, Dict, List

# ========================= Config =========================
GCP_PROJECT = "bigquery-sandbox-471123"
BQ_DATASET     = "dataset_fda"
BQ_TABLE_STAGE = "tirzepatide_events_stage"   # 3º passo (salva flat)
BQ_TABLE_COUNT = "openfda_tirzepatida"        # 4º passo (agrega diário)
BQ_LOCATION    = "US"
GCP_CONN_ID    = "google_cloud_default"

# Janela: exemplo de jan–jun/2025
TEST_START = date(2025, 1, 1)
TEST_END   = date(2025, 6, 30)
DRUG_QUERY = "tirzepatide"

API_LIMIT   = 1000
TIMEOUT_S   = 30
MAX_RETRIES = 3

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"})


# ========================= Helpers =========================
def _search_expr(day: date, drug_query: str) -> str:
    d = day.strftime("%Y%m%d")
    return f'patient.drug.openfda.generic_name:"{drug_query}" AND receivedate:{d}'

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    import time
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            time.sleep(attempt)  # backoff simples
            continue
        r.raise_for_status()

def _to_flat(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    flat: List[Dict[str, Any]] = []
    for ev in rows:
        patient   = (ev or {}).get("patient", {}) or {}
        reactions = patient.get("reaction", []) or []
        drugs     = patient.get("drug", []) or []
        flat.append({
            "safetyreportid":        ev.get("safetyreportid"),
            "receivedate":           ev.get("receivedate"),
            "patientsex":            patient.get("patientsex"),
            "primarysourcecountry":  ev.get("primarysourcecountry"),
            "serious":               ev.get("serious"),
            "reaction_pt":           (reactions[0].get("reactionmeddrapt") if reactions else None),
            "drug_product":          (drugs[0].get("medicinalproduct") if drugs else None),
        })
    df = pd.DataFrame(flat)
    if df.empty:
        return df
    df["safetyreportid"] = df["safetyreportid"].astype(str)
    df["receivedate"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce").dt.date
    for col in ["patientsex", "serious"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset=["safetyreportid"], keep="first")
    return df


# ========================= DAG =========================
@dag(
    dag_id="openfda_tirzepatida_stage_pipeline",
    description="Consulta openFDA (tirzepatide) -> trata (flat) -> salva (BQ stage) -> agrega diário (BQ).",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["didatico", "openfda", "faers", "bigquery", "etl"],
)
def openfda_tirzepatida_stage_pipeline():

    # grava direto no BQ em append; não retorna dados (evita XCom gigante)
    @task(do_xcom_push=False, queue="heavy", retries=2)
    def stage_from_openfda() -> Dict[str, str]:
        base_url = "https://api.fda.gov/drug/event.json"

        # esquema fixo p/ garantir tipos no BQ
        schema = [
            {"name": "safetyreportid",       "type": "STRING"},
            {"name": "receivedate",          "type": "DATE"},
            {"name": "patientsex",           "type": "INTEGER"},
            {"name": "primarysourcecountry", "type": "STRING"},
            {"name": "serious",              "type": "INTEGER"},
            {"name": "reaction_pt",          "type": "STRING"},
            {"name": "drug_product",         "type": "STRING"},
        ]

        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        creds = bq.get_credentials()

        first_write = True
        total = 0
        day = TEST_START

        while day <= TEST_END:
            skip = 0
            wrote_day = 0

            while True:
                params = {
                    "search": _search_expr(day, DRUG_QUERY),
                    "limit": str(API_LIMIT),
                    "skip": str(skip),
                }
                print(f"[fetch] Dia {day} | limit={API_LIMIT} skip={skip} | {params['search']}")
                payload = _openfda_get(base_url, params)
                rows = payload.get("results", []) or []
                if not rows:
                    break

                # normaliza só o lote atual (economia de RAM)
                df = _to_flat(rows)
                if not df.empty:
                    pandas_gbq.to_gbq(
                        df,
                        destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
                        project_id=GCP_PROJECT,
                        if_exists=("replace" if first_write else "append"),
                        credentials=creds,
                        table_schema=schema,
                        location=BQ_LOCATION,
                        progress_bar=False,
                        chunksize=10_000,  # grava em lotes no BQ
                    )
                    first_write = False
                    wrote_day += len(df)
                    total += len(df)

                # próximo lote no mesmo dia
                skip += API_LIMIT
                if len(rows) < API_LIMIT:
                    break

            print(f"[stage] {day}: {wrote_day} linhas (acumulado {total}).")
            day += timedelta(days=1)

        print(f"[stage] Janela {TEST_START}–{TEST_END}: {total} linhas gravadas em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}.")
        # retorna só metadados pequenos (XCom pequeno)
        return {"start": TEST_START.strftime("%Y-%m-%d"),
                "end":   TEST_END.strftime("%Y-%m-%d"),
                "drug":  DRUG_QUERY}

    @task(retries=2)
    def build_daily_counts(meta: Dict[str, str]) -> None:
        start, end, drug = meta["start"], meta["end"], meta["drug"]

        sql = f"""
        SELECT
          receivedate AS day,
          COUNT(*)    AS events,
          '{drug}'    AS drug
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}`
        WHERE receivedate BETWEEN DATE('{start}') AND DATE('{end}')
        GROUP BY day
        ORDER BY day
        """
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        creds = bq.get_credentials()

        df_counts = pandas_gbq.read_gbq(
            sql,
            project_id=GCP_PROJECT,
            credentials=creds,
            dialect="standard",
            location=BQ_LOCATION,
            progress_bar_type=None,
        )
        if df_counts.empty:
            print("[counts] Nenhuma linha para agregar.")
            return

        schema_counts = [
            {"name": "day",    "type": "DATE"},
            {"name": "events", "type": "INTEGER"},
            {"name": "drug",   "type": "STRING"},
        ]
        pandas_gbq.to_gbq(
            df_counts,
            destination_table=f"{BQ_DATASET}.{BQ_TABLE_COUNT}",
            project_id=GCP_PROJECT,
            if_exists="replace",
            credentials=creds,
            table_schema=schema_counts,
            location=BQ_LOCATION,
            progress_bar=False,
            chunksize=10_000,
        )
        print(f"[counts] {len(df_counts)} linhas gravadas em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_COUNT}.")

    build_daily_counts(stage_from_openfda())

openfda_tirzepatida_stage_pipeline()
