# dags/atorvastatin.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from google.cloud import bigquery
import pendulum
import pandas as pd
import requests
import time
from datetime import date
from typing import Any, Dict, List

# ========================= Config =========================
GCP_PROJECT     = "bigquery-sandbox-471123"          # seu projeto GCP
BQ_DATASET      = "dataset_fda"                  # dataset de destino
BQ_TABLE_STAGE  = "atorvastatin_ae_stage"
BQ_TABLE_COUNT  = "atorvastatin_ae_daily"
BQ_LOCATION     = "US"                       # região do dataset
GCP_CONN_ID     = "google_cloud_default"     # conexão no Astronomer

# Jun -> Jul/2022 (inclusive)
TEST_START = date(2022, 6, 1)
TEST_END   = date(2022, 7, 31)

DRUG_GENERIC = "atorvastatin"  # filtro do medicamento

TIMEOUT_S   = 30
MAX_RETRIES = 3

# HTTP session
SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"}
)

# ========================= Helpers =========================
def _search_expr_by_day(day: date, generic_name: str) -> str:
    d = day.strftime("%Y%m%d")
    return f'patient.drug.medicinalproduct:"{generic_name}" AND receivedate:{d}'

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    """GET com retentativas para a API openFDA."""
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        # log simples de erro
        try:
            print("[openFDA][err]", r.status_code, r.json())
        except Exception:
            print("[openFDA][err-text]", r.status_code, r.text[:500])
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            time.sleep(attempt)  # backoff linear simples
            continue
        r.raise_for_status()

def _to_flat_drug(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    flat: List[Dict[str, Any]] = []
    for ev in rows:
        patient = ev.get("patient", {})
        drugs = patient.get("drug", [])
        drug0 = drugs[0] if drugs else {}
        flat.append({
            "safetyreportid": ev.get("safetyreportid"),
            "receivedate": ev.get("receivedate"),
            "reaction": (ev.get("reaction", [{}])[0] or {}).get("reactionmeddrapt"),
            "drugname": drug0.get("medicinalproduct"),
            "drugindication": drug0.get("drugindication"),
            "patientsex": patient.get("patientsex"),
            "patientage": patient.get("patientonsetage"),
            "patientageunit": patient.get("patientonsetageunit"),
        })
    df = pd.DataFrame(flat)
    if df.empty:
        return df
    df["safetyreportid"] = df["safetyreportid"].astype(str)
    df["receivedate"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce").dt.date
    df = df.drop_duplicates(subset=["safetyreportid"], keep="first")
    return df

def _ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str) -> None:
    ds_id = f"{project}.{dataset}"
    ds = bigquery.Dataset(ds_id)
    ds.location = location
    client.create_dataset(ds, exists_ok=True)  # idempotente

# ========================= DAG =========================
@dag(
    dag_id="openfda_atorvastatin_ae_pipeline",
    description=(
        "openFDA drug/event (atorvastatin) → "
        "normaliza (STAGE) → agrega diário (BQ). Jun–Jul/2022."
    ),
    # ...existing code...
)
def openfda_atorvastatin_ae_pipeline():

    @task(retries=0)
    def extract_transform_load() -> Dict[str, str]:
        """
        ETL completo no mesmo task para evitar XCom grande:
        - Busca dia a dia, com paginação (limit=1000, skip)
        - Normaliza (flat)
        - Grava STAGE no BigQuery com WRITE_TRUNCATE (idempotente)
        - Retorna metadados mínimos para o próximo task
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
                    "search": _search_expr_by_day(day, DRUG_GENERIC),
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
                time.sleep(0.25)  # respeita rate limit
            print(f"[fetch] {day}: {total_dia} registros.")
            day = date.fromordinal(day.toordinal() + 1)
        print(f"[fetch] Jun–jul/2022: {n_calls} chamadas, {len(all_rows)} registros no total.")

        # Normaliza
        df = _to_flat_drug(all_rows)
        print(f"[normalize] linhas pós-normalização: {len(df)}")
        if not df.empty:
            print("[normalize] preview:\n", df.head(10).to_string(index=False))

        # Cliente BigQuery
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
        client: bigquery.Client = bq_hook.get_client()
        _ensure_dataset(client, GCP_PROJECT, BQ_DATASET, BQ_LOCATION)

        # Esquema da STAGE
        schema_stage = [
            bigquery.SchemaField("safetyreportid", "STRING"),
            bigquery.SchemaField("receivedate", "DATE"),
            bigquery.SchemaField("reaction", "STRING"),
            bigquery.SchemaField("drugname", "STRING"),
            bigquery.SchemaField("drugindication", "STRING"),
            bigquery.SchemaField("patientsex", "STRING"),
            bigquery.SchemaField("patientage", "STRING"),
            bigquery.SchemaField("patientageunit", "STRING"),
        ]

        # Dropar tabela STAGE para remover propriedades antigas (idempotente)
        table_id_stage = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}"
        client.delete_table(table_id_stage, not_found_ok=True)

        # DataFrame vazio com colunas corretas, se necessário
        if df.empty:
            df = pd.DataFrame({f.name: pd.Series(dtype="object") for f in schema_stage})
            df["receivedate"] = pd.to_datetime(pd.Series([], dtype="datetime64[ns]"))

        job_config_stage = bigquery.LoadJobConfig(
            schema=schema_stage,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_dataframe(
            dataframe=df,
            destination=table_id_stage,
            job_config=job_config_stage,
            location=BQ_LOCATION,
        )
        load_job.result()
        print(f"[stage] Gravados {len(df)} registros em {table_id_stage}.")

        return {
            "start":  TEST_START.strftime("%Y-%m-%d"),
            "end":    TEST_END.strftime("%Y-%m-%d"),
            "generic": DRUG_GENERIC,
        }

    @task(retries=0)
    def build_daily_counts(meta: Dict[str, str]) -> None:
        """
        Agrega contagem diária a partir da STAGE e salva a tabela final particionada por event_date.
        """
        start, end, generic = meta["start"], meta["end"], meta["generic"]

        query_sql = f"""
        SELECT
          receivedate AS event_date,
          COUNT(*)      AS events,
          '{generic}'   AS drug_generic
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}`
        WHERE receivedate BETWEEN DATE('{start}') AND DATE('{end}')
        GROUP BY event_date
        ORDER BY event_date
        """

        # Cliente BigQuery
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
        client: bigquery.Client = bq_hook.get_client()
        _ensure_dataset(client, GCP_PROJECT, BQ_DATASET, BQ_LOCATION)

        # Executa a consulta e converte para DataFrame
        query_job = client.query(query_sql, location=BQ_LOCATION)
        df_counts = query_job.result().to_dataframe(create_bqstorage_client=False)

        if df_counts.empty:
            print("[counts] Nenhuma linha para agregar.")
            df_counts = pd.DataFrame(
                {
                    "event_date": pd.Series(dtype="datetime64[ns]"),
                    "events": pd.Series(dtype="int64"),
                    "drug_generic": pd.Series(dtype="object"),
                }
            )
        else:
            # Garantir tipo datetime para conversão a DATE
            df_counts["event_date"] = pd.to_datetime(df_counts["event_date"], errors="coerce")

        schema_counts = [
            bigquery.SchemaField("event_date",     "DATE"),
            bigquery.SchemaField("events",         "INTEGER"),
            bigquery.SchemaField("drug_generic", "STRING"),
        ]

        table_id_counts = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_COUNT}"

        #Remover a tabela antes para descartar partições antigas com campo diferente
        client.delete_table(table_id_counts, not_found_ok=True)

        job_config_counts = bigquery.LoadJobConfig(
            schema=schema_counts,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_date",   # o campo de partição EXISTE no schema acima
            ),
        )

        load_job2 = client.load_table_from_dataframe(
            dataframe=df_counts,
            destination=table_id_counts,
            job_config=job_config_counts,
            location=BQ_LOCATION,
        )
        load_job2.result()
        print(f"[counts] {len(df_counts)} linhas gravadas em {table_id_counts}.")

    build_daily_counts(extract_transform_load())

openfda_atorvastatin_ae_pipeline()
