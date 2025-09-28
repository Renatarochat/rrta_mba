from __future__ import annotations

import logging
import time
from collections import Counter
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


# --------------------------- DAG ---------------------------

@dag(
    dag_id=DAG_ID,
    description="Coletar eventos OpenFDA (Atorvastatin/Lipitor) por mês e salvar contagem diária no BigQuery",
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
        Busca eventos crus (apenas campos receiptdate/receivedate) com paginação e
        retorna lista de dicionários desses campos para o mês.
        Faz OR entre receiptdate e receivedate para cobrir ambas as datas.
        """
        ctx = get_current_context()
        first_day, last_day = _month_bounds(ctx)
        start_ds, end_ds = _ds(first_day), _ds(last_day)
        logging.info("Janela consultada: %s..%s", start_ds, end_ds)

        # Filtro amplo para aumentar recall (sem .exact / case-insensitive)
        product_filter = (
            '('
            'patient.drug.medicinalproduct:(atorvastatin OR "ATORVASTATIN" OR lipitor) OR '
            'patient.drug.activesubstance.activesubstancename:(ATORVASTATIN) OR '
            'patient.drug.openfda.substance_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.generic_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.brand_name:(LIPITOR)'
            ')'
        )
        window_filter = f"(receiptdate:[{start_ds} TO {end_ds}] OR receivedate:[{start_ds} TO {end_ds}])"
        search = f"{product_filter} AND {window_filter}"

        rows: List[Dict[str, Any]] = []
        limit = 100
        skip = 0
        # pagina até esgotar (o volume para um único fármaco por mês é normalmente baixo)
        while True:
            page = _openfda_get(
                {
                    "search": search,
                    "limit": limit,
                    "skip": skip,
                    "fields": "receiptdate,receivedate",
                    "sort": "receiptdate:asc",
                },
                timeout=60,
            )
            batch = (page or {}).get("results", [])
            if not batch:
                break
            for ev in batch:
                # mantemos só os campos de interesse
                rcv = ev.get("receiptdate")
                rcvd = ev.get("receivedate")
                if rcv or rcvd:
                    rows.append({"receiptdate": rcv, "receivedate": rcvd})
            if len(batch) < limit:
                break
            skip += limit

        logging.info("Eventos brutos coletados: %d", len(rows))
        return rows

    @task(task_id="transform_events")
    def transform_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Agrega por dia (preferindo 'receiptdate'; se ausente, usa 'receivedate').
        Saída: [{"receivedate":"YYYY-MM-DD","count":N}, ...]
        """
        counter: Counter[str] = Counter()
        for ev in events or []:
            s = str(ev.get("receiptdate") or ev.get("receivedate") or "")
            if not s:
                continue
            try:
                if len(s) == 8 and s.isdigit():
                    dt = datetime.strptime(s, "%Y%m%d").date()
                else:
                    dt = datetime.fromisoformat(s).date()
            except Exception:
                continue
            counter[dt.isoformat()] += 1

        out = [{"receivedate": d, "count": int(n)} for d, n in sorted(counter.items())]
        logging.info("Dias agregados: %d", len(out))
        return out

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(rows: List[Dict[str, Any]]) -> None:
        """
        Grava no BigQuery (dataset_fda.drug_events_atorvastatin_daily), particionado por dia.
        """
        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_atorvastatin_daily"
        gcp_conn_id = "google_cloud_default"

        if not rows:
            logging.info("Nenhum dado agregado para carregar. Nada a fazer.")
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

    fetched = fetch_openfda()
    transformed = transform_events(fetched)
    save_to_bigquery(transformed)


dag = atorvastatin_openfda_monthly_to_bq()
