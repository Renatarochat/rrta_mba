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


DAG_ID = "openfda_atorvastatin_monthly_to_bq_diagnostic"
OPENFDA_ENDPOINT = "https://api.fda.gov/drug/event.json"
ALLOW_EMPTY = True  # mantém True para sempre salvar probe/amostra mesmo sem série


# --------------------------- Helpers ---------------------------

def _openfda_get(endpoint: str, params: Dict[str, Any], timeout: int = 60) -> Optional[Dict[str, Any]]:
    """GET tolerante: 404 -> None; 429/5xx -> retries leves; demais -> raise."""
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


def _month_bounds_from_year_month(y: int, m: int) -> Tuple[date, date]:
    first = date(y, m, 1)
    next_first = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    last = next_first - timedelta(days=1)
    return first, last


def _resolve_window(ctx: Dict[str, Any]) -> Tuple[date, date]:
    """
    Janela (primeiro_dia, ultimo_dia).
      1) dag_run.conf: {"year": 2023, "month": 5}
      2) dag_run.conf: {"start": "YYYYMMDD", "end": "YYYYMMDD"}
      3) mês de (data_interval_end - 1 dia)
    """
    conf: Dict[str, Any] = {}
    dag_run = ctx.get("dag_run")
    try:
        if dag_run is not None and getattr(dag_run, "conf", None):
            conf = dag_run.conf if isinstance(dag_run.conf, dict) else dict(dag_run.conf)
    except Exception:
        conf = {}

    y = conf.get("year")
    m = conf.get("month")
    if isinstance(y, int) and isinstance(m, int) and 1 <= m <= 12:
        return _month_bounds_from_year_month(y, m)

    start = conf.get("start")
    end = conf.get("end")
    if isinstance(start, str) and isinstance(end, str) and len(start) == 8 and len(end) == 8:
        try:
            first = datetime.strptime(start, "%Y%m%d").date()
            last = datetime.strptime(end, "%Y%m%d").date()
            if first <= last:
                return first, last
        except Exception:
            pass

    data_interval_end: datetime = ctx["data_interval_end"]
    last = (data_interval_end - timedelta(days=1)).date()
    first = last.replace(day=1)
    return first, last


def _ds(d: date) -> str:
    return d.strftime("%Y%m%d")


# --------------------------- DAG ---------------------------

@dag(
    dag_id=DAG_ID,
    description="Série diária de eventos FAERS para Atorvastatina (Lipitor) com artefatos de diagnóstico",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=True,
    default_args={"owner": "data-eng"},
    tags=["openfda", "atorvastatin", "lipitor", "bigquery", "diagnostic"],
)
def atorvastatin_openfda_monthly_to_bq_diagnostic():
    @task(task_id="fetch_openfda", retries=2, retry_delay=timedelta(minutes=5))
    def fetch_openfda() -> Dict[str, Any]:
        """
        Retorna:
        {
          "counts": [{"time":"YYYYMMDD","count":N}, ...],
          "probe": { "has_data": bool, "received_pts": int, "receipt_pts": int,
                     "window_start":"YYYYMMDD","window_end":"YYYYMMDD" },
          "sample": [ {..evento cru..}, ... ]   # até 200 eventos
        }
        """
        ctx = get_current_context()
        first_day, last_day = _resolve_window(ctx)
        start_ds, end_ds = _ds(first_day), _ds(last_day)
        logging.info("Janela consultada: %s..%s", start_ds, end_ds)

        # Filtro amplo (sem .exact) p/ maior recall
        product_filter = (
            '('
            'patient.drug.medicinalproduct:(atorvastatin OR lipitor) OR '
            'patient.drug.openfda.substance_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.brand_name:(LIPITOR)'
            ')'
        )
        rcv_range = f"receivedate:[{start_ds} TO {end_ds}]"
        rcp_range = f"receiptdate:[{start_ds} TO {end_ds}]"

        # A) count por receivedate
        params_a = {"search": f"{product_filter} AND {rcv_range}", "count": "receivedate", "limit": 1000}
        data_a = _openfda_get(OPENFDA_ENDPOINT, params_a, timeout=60)
        if data_a and isinstance(data_a.get("results"), list) and data_a["results"]:
            counts = data_a["results"]
        else:
            # B) count por receiptdate
            params_b = {"search": f"{product_filter} AND {rcp_range}", "count": "receiptdate", "limit": 1000}
            data_b = _openfda_get(OPENFDA_ENDPOINT, params_b, timeout=60)
            counts = data_b["results"] if data_b and isinstance(data_b.get("results"), list) else []

        # C) PROBE sem filtro de produto
        probe_recv = _openfda_get(OPENFDA_ENDPOINT, {"search": rcv_range, "count": "receivedate"}, timeout=45)
        recv_pts = len((probe_recv or {}).get("results", []) or [])
        probe_rcpt = _openfda_get(OPENFDA_ENDPOINT, {"search": rcp_range, "count": "receiptdate"}, timeout=45)
        rcpt_pts = len((probe_rcpt or {}).get("results", []) or [])
        has_data = (recv_pts + rcpt_pts) > 0

        # D) Amostra crua (até 200 eventos)
        sample_limit_total = 200
        per_page = 100
        collected = 0
        sample_rows: List[Dict[str, Any]] = []
        skip = 0
        while collected < sample_limit_total:
            page = _openfda_get(
                OPENFDA_ENDPOINT,
                {"search": f"{product_filter} AND ({rcv_range} OR {rcp_range})", "limit": per_page, "skip": skip},
                timeout=60,
            )
            batch = (page or {}).get("results", [])
            if not batch:
                break
            sample_rows.extend(batch)
            collected += len(batch)
            if len(batch) < per_page:
                break
            skip += per_page

        logging.info("Counts=%d, probe(received)=%d, probe(receipt)=%d, sample=%d",
                     len(counts), recv_pts, rcpt_pts, len(sample_rows))

        return {
            "counts": counts or [],
            "probe": {
                "has_data": has_data,
                "received_pts": recv_pts,
                "receipt_pts": rcpt_pts,
                "window_start": start_ds,
                "window_end": end_ds,
            },
            "sample": sample_rows[:sample_limit_total],
        }

    @task(task_id="transform_counts")
    def transform_counts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        results = payload.get("counts") or []
        out: List[Dict[str, Any]] = []
        for row in results:
            s = str(row.get("receivedate", row.get("time")))
            c = row.get("count")
            if s is None or c is None:
                continue
            try:
                if len(s) == 8 and s.isdigit():
                    dt = datetime.strptime(s, "%Y%m%d").date()
                else:
                    dt = datetime.fromisoformat(s).date()
                cnt = int(c)
            except Exception:
                continue
            out.append({"receivedate": dt.isoformat(), "count": cnt})
        logging.info("Transform → %d linhas.", len(out))
        return out

    @task(task_id="save_counts_bq")
    def save_counts_bq(rows: List[Dict[str, Any]]) -> None:
        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_atorvastatin_daily"
        gcp_conn_id = "google_cloud_default"

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

        if not rows:
            logging.info("Sem linhas de contagem para carregar (counts=0).")
            return

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

    @task(task_id="save_probe_bq")
    def save_probe_bq(payload: Dict[str, Any]) -> None:
        """Salva SEMPRE a telemetria do mês (útil quando a série vem vazia)."""
        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_atorvastatin_probe"
        gcp_conn_id = "google_cloud_default"

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
            bigquery.SchemaField("window_start", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("window_end", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("has_data", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("received_pts", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("receipt_pts", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("counts_points", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("sample_rows", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            client.get_table(table_ref)
        except Exception:
            client.create_table(bigquery.Table(table_ref, schema=schema))

        probe = payload.get("probe") or {}
        counts_points = len(payload.get("counts") or [])
        sample_rows = len(payload.get("sample") or [])

        row = {
            "window_start": probe.get("window_start", ""),
            "window_end": probe.get("window_end", ""),
            "has_data": bool(probe.get("has_data", False)),
            "received_pts": int(probe.get("received_pts", 0)),
            "receipt_pts": int(probe.get("receipt_pts", 0)),
            "counts_points": counts_points,
            "sample_rows": sample_rows,
            "ingested_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }

        from io import BytesIO
        import json
        buf = BytesIO((json.dumps(row) + "\n").encode("utf-8"))
        job = client.load_table_from_file(
            buf,
            table_ref,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ),
        )
        job.result()
        logging.info("Probe salvo em %s.%s.%s", project_id, dataset_id, table_id)

    @task(task_id="save_sample_bq")
    def save_sample_bq(payload: Dict[str, Any]) -> None:
        """Salva até 200 eventos crus para inspeção do filtro."""
        sample = payload.get("sample") or []
        if not sample:
            logging.info("Nenhuma amostra para salvar.")
            return

        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_atorvastatin_sample"
        gcp_conn_id = "google_cloud_default"

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
        # Usamos STRING para máxima compatibilidade de sandbox
        schema = [
            bigquery.SchemaField("raw_event", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("receivedate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("receiptdate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            client.get_table(table_ref)
        except Exception:
            client.create_table(bigquery.Table(table_ref, schema=schema))

        from io import BytesIO
        import json

        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        buf = BytesIO()
        for ev in sample:
            buf.write((
                json.dumps({
                    "raw_event": json.dumps(ev, ensure_ascii=False),
                    "receivedate": ev.get("receivedate"),
                    "receiptdate": ev.get("receiptdate"),
                    "ingested_at": now,
                }) + "\n"
            ).encode("utf-8"))
        buf.seek(0)

        job = client.load_table_from_file(
            buf,
            table_ref,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ),
        )
        job.result()
        logging.info("Amostra (%d eventos) salva em %s.%s.%s", len(sample), project_id, dataset_id, table_id)

    payload = fetch_openfda()
    rows = transform_counts(payload)
    save_counts_bq(rows)
    save_probe_bq(payload)
    save_sample_bq(payload)


dag = atorvastatin_openfda_monthly_to_bq_diagnostic()
