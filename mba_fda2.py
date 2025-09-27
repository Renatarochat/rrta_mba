# dags/openfda_semaglutina_stage_pipeline.py
from _future_ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pendulum
import pandas as pd
import pandas_gbq
import requests
import time
from datetime import date
from typing import Any, Dict, List

# ========================= Config =========================
GCP_PROJECT    = "bigquery-sandbox-471123"
BQ_DATASET     = "dataset_fda"
BQ_TABLE_STAGE = "semaglutide_events_stage"   # stage (flat)
BQ_TABLE_COUNT = "openfda_semaglutina"        # contagem diária
BQ_LOCATION    = "US"
GCP_CONN_ID    = "google_cloud_default"

# Jan -> Jun/2025 (inclusive)
TEST_START = date(2025, 1, 1)
TEST_END   = date(2025, 6, 30)
DRUG_QUERY = "semaglutide"

TIMEOUT_S   = 30
MAX_RETRIES = 3

# HTTP session
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"})


# ========================= Helpers =========================
def _search_expr_by_day(day: date, drug_query: str) -> str:
    d = day.strftime("%Y%m%d")
    return f'patient.drug.openfda.generic_name:"{drug_query}" AND receivedate:{d}'

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        try:
            print("[openFDA][err]", r.status_code, r.json())
        except Exception:
            print("[openFDA][err-text]", r.status_code, r.text[:500])
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            time.sleep(attempt)
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
    dag_id="openfda_semaglutina_stage_pipeline",
    description="Consulta openFDA (semaglutide) -> trata (flat) -> salva (BQ stage) -> agrega diário (BQ).",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["didatico", "openfda", "faers", "bigquery", "etl"],
)
def openfda_semaglutina_stage_pipeline():

    @task(retries=0)
    def extract_transform_load() -> Dict[str, str]:
        """
        ETL completo no mesmo task para evitar XCom gigante:
        - Busca dia a dia com paginação (limit=1000, skip)
        - Normaliza (flat)
        - Grava STAGE com if_exists='replace'
        - Retorna só metadados (pequeno) para o próximo task
        """
        base_url = "https://api.fda.gov/drug/event.json"
        all_rows: List[Dict[str, Any]] = []

        day = TEST_START
        n_calls = 0
        while day <= TEST_END:
            limit = 1000
            skip = 0
            total_dia = 0
            while True:
                params = {
                    "search": _search_expr_by_day(day, DRUG_QUERY),
                    "limit": str(limit),
                    "skip": str(skip),
                }
                payload = _openfda_get(base_url, params)
                rows = payload.get("results", []) or []
                all_rows.extend(rows)
                total_dia += len(rows)
                n_calls += 1
                if len(rows) < limit:
                    break
                skip += limit
                time.sleep(0.25)  # rate-limit
            print(f"[fetch] {day}: {total_dia} registros.")
            day = date.fromordinal(day.toordinal() + 1)
        print(f"[fetch] Jan–Jun/2025: {n_calls} chamadas, {len(all_rows)} registros no total.")

        # Normalize + load stage
        df = _to_flat(all_rows)
        print(f"[normalize] linhas pós-normalização: {len(df)}")
        if not df.empty:
            print("[normalize] preview:\n", df.head(10).to_string(index=False))

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
        if df.empty:
            # Mesmo assim, "zera" a stage para a janela (replace em vazio)
            empty = pd.DataFrame(columns=[c["name"] for c in schema])
            empty.to_gbq(
                destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
                project_id=GCP_PROJECT,
                if_exists="replace",
                credentials=creds,
                table_schema=schema,
                location=BQ_LOCATION,
                progress_bar=False,
            )
            print("[stage] Nenhum registro para gravar, stage substituída por tabela vazia.")
        else:
            df.to_gbq(
                destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
                project_id=GCP_PROJECT,
                if_exists="replace",
                credentials=creds,
                table_schema=schema,
                location=BQ_LOCATION,
                progress_bar=False,
            )
            print(f"[stage] Gravados {len(df)} em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}.")

        return {"start": TEST_START.strftime("%Y-%m-%d"),
                "end":   TEST_END.strftime("%Y-%m-%d"),
                "drug":  DRUG_QUERY}

    @task(retries=0)
    def build_daily_counts(meta: Dict[str, str]) -> None:
        start, end, drug = meta["start"], meta["end"], meta["drug"]

        sql = f"""
        SELECT
          receivedate AS day,
          COUNT(*)    AS events,
          '{drug}'    AS drug
        FROM {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}
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
            progress_bar_type=None,
            location=BQ_LOCATION,
        )
        if df_counts.empty:
            print("[counts] Nenhuma linha para agregar.")
            # ainda assim, cria a tabela de counts vazia
            df_counts = pd.DataFrame(columns=["day","events","drug"])

        schema_counts = [
            {"name": "day",    "type": "DATE"},
            {"name": "events", "type": "INTEGER"},
            {"name": "drug",   "type": "STRING"},
        ]
        df_counts.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE_COUNT}",
            project_id=GCP_PROJECT,
            if_exists="replace",
            credentials=creds,
            table_schema=schema_counts,
            location=BQ_LOCATION,
            progress_bar=False,
        )
        print(f"[counts] {len(df_counts)} linhas gravadas em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_COUNT}.")

    build_daily_counts(extract_transform_load())

openfda_semaglutina_stage_pipeline()
