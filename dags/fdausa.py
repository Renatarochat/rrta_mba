from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


DAG_ID = "openfda_tirzepatide_monthly_to_bq"
ALLOW_EMPTY = False  # coloque True se quiser que o DAG “verde” mesmo sem dados


def _openfda_get(endpoint: str, params: Dict[str, Any], timeout: int = 60) -> Optional[Dict[str, Any]]:
    """GET com tolerância: 404 -> None; 429/5xx com retries simples."""
    headers = {"User-Agent": f"airflow-dag/{DAG_ID}"}
    for attempt in range(3):
        resp = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
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


def _month_bounds(exec_end: datetime) -> tuple[date, date]:
    """(primeiro_dia, ultimo_dia) do mês de (data_interval_end - 1 dia)."""
    last_day = (exec_end - timedelta(days=1)).date()
    first_day = last_day.replace(day=1)
    return first_day, last_day


@dag(
    dag_id=DAG_ID,
    description="Coletar eventos OpenFDA de tirzepatida por 1 mês e salvar no BigQuery para série histórica",
    start_date=datetime(2025, 8, 1),
    schedule="@monthly",
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["openfda", "tirzepatide", "bigquery"],
)
def tirzepatide_openfda_monthly_to_bq():
    @task(task_id="fetch_openfda", retries=2, retry_delay=timedelta(minutes=5))
    def fetch_openfda() -> List[Dict[str, Any]]:
        """
        1) Tenta agregação nativa (count=receivedate).
        2) Se vazio, pagina eventos e agrega localmente por receivedate (fallback para receiptdate).
        Retorno sempre no formato [{"time": "YYYYMMDD", "count": N}, ...].
        """
        ctx = get_current_context()
        first_day, last_day = _month_bounds(ctx["data_interval_end"])
        start_ds = first_day.strftime("%Y%m%d")
        end_ds = last_day.strftime("%Y%m%d")

        endpoint = "https://api.fda.gov/drug/event.json"

        # Filtro simples e compatível (evita 400): medicinalproduct + marcas + substância
        product_filter = (
            '('
            'patient.drug.medicinalproduct:("tirzepatide" OR "Mounjaro" OR "Zepbound") '
            'OR patient.drug.openfda.brand_name.exact:("MOUNJARO" OR "ZEPBOUND") '
            'OR patient.drug.openfda.substance_name.exact:("TIRZEPATIDE")'
            ')'
        )
        date_range = f"receivedate:[{start_ds} TO {end_ds}]"
        base_search = f"{product_filter} AND {date_range}"

        # (1) Agregação nativa
        params_count = {"search": base_search, "count": "receivedate", "limit": 1000}
        data = _openfda_get(endpoint, params_count, timeout=60)
        if data and isinstance(data.get("results"), list) and data["results"]:
            logging.info("OpenFDA (count=receivedate) retornou %d linhas.", len(data["results"]))
            return data["results"]

        # (2) Paginação de eventos e agregação local
        limit = 100
        skip = 0
        counts: Dict[str, int] = {}
        total = 0

        while True:
            params_page = {
                "search": base_search,  # sem fields/sort para evitar 400
                "limit": limit,
                "skip": skip,
            }
            page = _openfda_get(endpoint, params_page, timeout=60)
            results = (page or {}).get("results", [])
            if not results:
                break

            for ev in results:
                # Preferimos 'receivedate'; se ausente, caímos para 'receiptdate'
                rd = ev.get("receivedate") or ev.get("receiptdate")
                if rd:
                    key = str(rd)
                    counts[key] = counts.get(key, 0) + 1
                    total += 1

            if len(results) < limit:
                break
            skip += limit

        out = [{"time": k, "count": v} for k, v in sorted(counts.items())]
        logging.info("Fallback agregou %d eventos em %d dias distintos.", total, len(out))
        return out

    @task(task_id="transform_events")
    def transform_events(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in results or []:
            received = row.get("receivedate", row.get("time"))
            cnt = row.get("count")
            if received is None or cnt is None:
                continue

            s = str(received)
            try:
                if len(s) == 8 and s.isdigit():
                    dt = datetime.strptime(s, "%Y%m%d").date()
                else:
                    dt = datetime.fromisoformat(s).date()
            except Exception:
                continue

            try:
                c = int(cnt)
            except Exception:
                continue

            out.append({"receivedate": dt.isoformat(), "count": c})

        logging.info("Transform produziu %d linhas.", len(out))
        return out

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(rows: List[Dict[str, Any]]) -> None:
        if not rows:
            msg = "Nenhum dado para carregar no BigQuery (consulta OpenFDA retornou 0)."
            if ALLOW_EMPTY:
                logging.info(msg)
                return
            raise RuntimeError(msg)

        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_tirzepatide_daily"
        gcp_conn_id = "google_cloud_default"

        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
        client: bigquery.Client = bq_hook.get_client(project_id=project_id)

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            ds = bigquery.Dataset(dataset_ref)
            ds.location = "US"
            client.create_dataset(ds, exists_ok=True)

        table_ref = dataset_ref.table(table_id)
        schema = [
            bigquery.SchemaField("receivedate", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("count", "INT64", mode="REQUIRED"),
        ]
        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="receivedate"
        )

        try:
            client.get_table(table_ref)
        except Exception:
            tbl = bigquery.Table(table_ref, schema=schema)
            tbl.time_partitioning = time_partitioning
            client.create_table(tbl)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        import json
        from io import BytesIO

        buf = BytesIO()
        for r in rows:
            buf.write((json.dumps(r) + "\n").encode("utf-8"))
        buf.seek(0)

        job = client.load_table_from_file(buf, table_ref, job_config=job_config)
        job.result()
        logging.info("Carregadas %d linhas no BigQuery.", len(rows))

    fetched = fetch_openfda()
    transformed = transform_events(fetched)
    save_to_bigquery(transformed)


dag = tirzepatide_openfda_monthly_to_bq()
