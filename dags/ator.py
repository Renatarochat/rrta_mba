from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


DAG_ID = "openfda_atorvastatin_monthly_to_bq"
OPENFDA_ENDPOINT = "https://api.fda.gov/drug/event.json"


# --------------------------- Helpers ---------------------------

def _openfda_get(params: Dict[str, Any], timeout: int = 60) -> Optional[Dict[str, Any]]:
    """GET tolerante: 404 -> None; 429/5xx -> retries leves; demais -> raise."""
    headers = {"User-Agent": f"airflow-dag/{DAG_ID}"}
    for attempt in range(3):
        resp = requests.get(OPENFDA_ENDPOINT, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 404:
            return None
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 * (attempt + 1))
            continue
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return None
    return None


def _month_bounds(ctx: Dict[str, Any]) -> Tuple[date, date]:
    """
    Janela (primeiro_dia, ultimo_dia).
      1) dag_run.conf: {"year": 2023, "month": 4}
      2) mês de (data_interval_end - 1 dia)
    """
    first: date
    last: date
    conf = {}
    dag_run = ctx.get("dag_run")
    if dag_run is not None and getattr(dag_run, "conf", None):
        try:
            conf = dag_run.conf if isinstance(dag_run.conf, dict) else dict(dag_run.conf)
        except Exception:
            conf = {}

    y = conf.get("year")
    m = conf.get("month")
    if isinstance(y, int) and isinstance(m, int) and 1 <= m <= 12:
        first = date(y, m, 1)
        next_first = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
        last = next_first - timedelta(days=1)
        return first, last

    data_interval_end: datetime = ctx["data_interval_end"]
    last = (data_interval_end - timedelta(days=1)).date()
    first = last.replace(day=1)
    return first, last


def _ds(d: date) -> str:
    return d.strftime("%Y%m%d")


# --------------------------- DAG ---------------------------

@dag(
    dag_id=DAG_ID,
    description="Coletar eventos OpenFDA (Atorvastatin/Lipitor) por mês e salvar no BigQuery para série histórica",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=True,
    default_args={"owner": "data-eng"},
    tags=["openfda", "atorvastatin", "lipitor", "bigquery"],
)
def atorvastatin_openfda_monthly_to_bq():
    @task(task_id="fetch_openfda", retries=2, retry_delay=timedelta(minutes=5))
    def fetch_openfda() -> List[Dict[str, Any]]:
        """
        Busca contagem diária priorizando 'receiptdate' (data de recebimento no FDA).
        Se vier vazio, tenta 'receivedate'. Normaliza saída para {'receivedate': 'YYYY-MM-DD', 'count': N}.
        """
        ctx = get_current_context()
        first_day, last_day = _month_bounds(ctx)
        start_ds, end_ds = _ds(first_day), _ds(last_day)
        logging.info("Janela consultada: %s..%s", start_ds, end_ds)

        # Filtro amplo (não usa .exact para ganhar recall; case-insensitive)
        product_filter = (
            '('
            'patient.drug.medicinalproduct:(atorvastatin OR "ATORVASTATIN" OR lipitor) OR '
            'patient.drug.activesubstance.activesubstancename:(ATORVASTATIN) OR '
            'patient.drug.openfda.substance_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.generic_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.brand_name:(LIPITOR)'
            ')'
        )

        # 1) Agrega por receiptdate
        params_recpt = {
            "search": f"{product_filter} AND receiptdate:[{start_ds} TO {end_ds}]",
            "count": "receiptdate",
            "limit": 1000,
        }
        data = _openfda_get(params_recpt, timeout=60)
        results = (data or {}).get("results", []) if isinstance(data, dict) else []

        # 2) Se vazio, tenta por receivedate
        if not results:
            params_recv = {
                "search": f"{product_filter} AND receivedate:[{start_ds} TO {end_ds}]",
                "count": "receivedate",
                "limit": 1000,
            }
            data2 = _openfda_get(params_recv, timeout=60)
            results = (data2 or {}).get("results", []) if isinstance(data2, dict) else []

        # Normaliza para lista de dicts com 'receivedate' (nome canônico na nossa tabela)
        out: List[Dict[str, Any]] = []
        for row in results or []:
            t = row.get("time") or row.get("receivedate") or row.get("receiptdate")
            c = row.get("count")
            if t is None or c is None:
                continue
            s = str(t)
            try:
                if len(s) == 8 and s.isdigit():
                    dt = datetime.strptime(s, "%Y%m%d").date()
                else:
                    dt = datetime.fromisoformat(s).date()
                cnt = int(c)
            except Exception:
                continue
            out.append({"receivedate": dt.isoformat(), "count": cnt})

        logging.info("Registros normalizados: %d", len(out))
        return out

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(rows: List[Dict[str, Any]]) -> None:
        """
        Grava no BigQuery (dataset_fda.drug_events_atorvastatin_daily), particionado por dia.
        Mantém o nome da coluna 'receivedate' (mesmo quando a origem for 'receiptdate').
        """
        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_atorvastatin_daily"
        gcp_conn_id = "google_cloud_default"

        # Se vier vazio, apenas loga (não falha o DAG)
        if not rows:
            logging.info("Nenhum dado retornado pela OpenFDA para a janela. Nada a carregar.")
            return

        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
        client: bigquery.Client = bq_hook.get_client(project_id=project_id)

        ds_ref = bigquery.DatasetReference(project_id, dataset_id)
        try:
            client.get_dataset(ds_ref)
        except Exception:
            ds = bigquery.Dataset(ds_ref)
            ds.location = "US"
            client.create_dataset(ds, exists_ok=True)

        table_ref = ds_ref.table(table_id)

        schema = [
            bigquery.SchemaField("receivedate", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("count", "INT64", mode="REQUIRED"),
        ]
        time_part = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="receivedate")

        try:
            client.get_table(table_ref)
        except Exception:
            tbl = bigquery.Table(table_ref, schema=schema)
            tbl.time_partitioning = time_part
            client.create_table(tbl)

        # Carrega JSON Lines em memória
        from io import BytesIO
        import json

        buf = BytesIO()
        for r in rows:
            buf.write((json.dumps(r) + "\n").encode("utf-8"))
        buf.seek(0)

        job = client.load_table_from_file(
            buf,
            table_ref,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ),
        )
        job.result()
        logging.info("BigQuery: carregadas %d linhas em %s.%s.%s", len(rows), project_id, dataset_id, table_id)

    # flow
    series = fetch_openfda()
    save_to_bigquery(series)


dag = atorvastatin_openfda_monthly_to_bq()
