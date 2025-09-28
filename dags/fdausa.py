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


def _openfda_get(endpoint: str, params: Dict[str, Any], timeout: int = 60) -> Optional[Dict[str, Any]]:
    """GET com pequenas tolerâncias: 404 -> None; 429/5xx com retry simples."""
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
    """Retorna (primeiro_dia, ultimo_dia) do mês referente ao data_interval_end - 1 dia."""
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
        Tenta a agregação nativa (count=receivedate).
        Se vier vazia/404, faz fallback por dia (até 31 chamadas) somando counts.
        """
        ctx = get_current_context()
        first_day, last_day = _month_bounds(ctx["data_interval_end"])

        start_ds = first_day.strftime("%Y%m%d")
        end_ds = last_day.strftime("%Y%m%d")

        endpoint = "https://api.fda.gov/drug/event.json"

        # Filtro por produto (robusto, mas simples o suficiente para evitar 400s).
        product_filter = (
            '('
            'patient.drug.medicinalproduct:("tirzepatide" OR "Mounjaro" OR "Zepbound") '
            'OR patient.drug.openfda.substance_name.exact:("TIRZEPATIDE") '
            'OR patient.drug.openfda.brand_name.exact:("MOUNJARO" OR "ZEPBOUND")'
            ')'
        )
        date_range = f"receivedate:[{start_ds} TO {end_ds}]"
        base_search = f"{product_filter} AND {date_range}"

        # 1) Agregação nativa
        params_count = {"search": base_search, "count": "receivedate", "limit": 1000}
        data = _openfda_get(endpoint, params_count, timeout=60)
        if data and isinstance(data.get("results"), list) and data["results"]:
            logging.info("OpenFDA count retornou %d linhas.", len(data["results"]))
            return data["results"]

        # 2) Fallback: por dia (count por dia específico)
        out: List[Dict[str, Any]] = []
        cur = first_day
        while cur <= last_day:
            d = cur.strftime("%Y%m%d")
            search = f"{product_filter} AND receivedate:{d}"
            params_daily = {"search": search, "count": "receivedate", "limit": 1}
            daily = _openfda_get(endpoint, params_daily, timeout=30)
            results = (daily or {}).get("results", [])
            if isinstance(results, list) and results:
                # A API retorna [{"time":"YYYYMMDD", "count": N}]
                item = results[0]
                # Garante que a data é exatamente o dia consultado
                if str(item.get("time")) == d and int(item.get("count", 0)) > 0:
                    out.append({"time": d, "count": int(item["count"])})
            cur += timedelta(days=1)

        logging.info("Fallback diário produziu %d linhas.", len(out))
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

        logging.info("Transform gerou %d linhas.", len(out))
        return out

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(rows: List[Dict[str, Any]]) -> None:
        if not rows:
            logging.info("Nenhum dado para carregar no BigQuery.")
            return

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
        time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="receivedate")

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
