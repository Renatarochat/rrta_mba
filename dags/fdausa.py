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


DAG_ID = "openfda_atorvastatin_monthly_to_bq"
ALLOW_EMPTY = False  # mude para True se preferir não falhar quando não houver dados


def _openfda_get(endpoint: str, params: Dict[str, Any], timeout: int = 60) -> Optional[Dict[str, Any]]:
    """GET tolerante: 404 -> None; 429/5xx -> retries exponenciais leves; demais -> raise."""
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
    description="Coletar eventos OpenFDA de atorvastatina (Lipitor) por 1 mês e salvar no BigQuery para série histórica",
    start_date=datetime(2025, 8, 1),
    schedule="@monthly",
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["openfda", "atorvastatin", "lipitor", "bigquery"],
)
def atorvastatin_openfda_monthly_to_bq():
    @task(task_id="fetch_openfda", retries=2, retry_delay=timedelta(minutes=5))
    def fetch_openfda() -> List[Dict[str, Any]]:
        """
        Ordem de tentativa:
        (1) count=receivedate
        (2) count=receiptdate
        (3) paginação de eventos (receivedate OR receiptdate) com agregação local
        Retorna: [{"time": "YYYYMMDD", "count": N}, ...]
        """
        ctx = get_current_context()
        first_day, last_day = _month_bounds(ctx["data_interval_end"])
        start_ds = first_day.strftime("%Y%m%d")
        end_ds = last_day.strftime("%Y%m%d")

        endpoint = "https://api.fda.gov/drug/event.json"

        # Filtro para Atorvastatina (genérico, substância e marca)
        product_filter = (
            '('
            'patient.drug.openfda.substance_name.exact:("ATORVASTATIN CALCIUM" OR "ATORVASTATIN") OR '
            'patient.drug.openfda.generic_name.exact:("ATORVASTATIN") OR '
            'patient.drug.openfda.brand_name.exact:("LIPITOR") OR '
            'patient.drug.medicinalproduct:("atorvastatin" OR "Lipitor")'
            ')'
        )
        rcv_range = f"receivedate:[{start_ds} TO {end_ds}]"
        rcp_range = f"receiptdate:[{start_ds} TO {end_ds}]"

        # (1) Agregação nativa por receivedate
        params_count_recv = {"search": f"{product_filter} AND {rcv_range}", "count": "receivedate", "limit": 1000}
        data = _openfda_get(endpoint, params_count_recv, timeout=60)
        if data and isinstance(data.get("results"), list) and data["results"]:
            logging.info("OpenFDA count(receivedate) => %d linhas.", len(data["results"]))
            return data["results"]

        # (2) Agregação nativa por receiptdate
        params_count_rcpt = {"search": f"{product_filter} AND {rcp_range}", "count": "receiptdate", "limit": 1000}
        data2 = _openfda_get(endpoint, params_count_rcpt, timeout=60)
        if data2 and isinstance(data2.get("results"), list) and data2["results"]:
            logging.info("OpenFDA count(receiptdate) => %d linhas.", len(data2["results"]))
            return data2["results"]

        # (3) Paginação de eventos e agregação local (usa OR de datas).
        # Evitamos 'fields' e 'sort' para reduzir risco de 400.
        limit = 100
        skip = 0
        counts: Dict[str, int] = {}
        total = 0

        while True:
            params_page = {
                "search": f"{product_filter} AND ({rcv_range} OR {rcp_range})",
                "limit": limit,
                "skip": skip,
            }
            page = _openfda_get(endpoint, params_page, timeout=60)
            results = (page or {}).get("results", [])
            if not results:
                break

            for ev in results:
                raw_date = ev.get("receivedate") or ev.get("receiptdate")
                if not raw_date:
                    continue
                key = str(raw_date)
                counts[key] = counts.get(key, 0) + 1
                total += 1

            if len(results) < limit:
                break
            skip += limit

        out = [{"time": k, "count": v} for k, v in sorted(counts.items())]
        logging.info("Fallback paginado: %d eventos agregados em %d dias.", total, len(out))
        return out

    @task(task_id="transform_events")
    def transform_events(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normaliza para schema:
        - receivedate (DATE, YYYY-MM-DD)
        - count (INT64)
        """
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
        table_id = "drug_events_atorvastatin_daily"
        gcp_conn_id = "google_cloud_default"

        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
        client: bigquery.Client = bq_hook.get_client(project_id=project_id)

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            ds = bigquery.Dataset(dataset_ref)
            ds.location = "US"  # ajuste se seu projeto usar outra região
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


dag = atorvastatin_openfda_monthly_to_bq()
