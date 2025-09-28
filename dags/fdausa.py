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
ALLOW_EMPTY = False  # mude para True se quiser concluir o DAG mesmo sem dados
OPENFDA_ENDPOINT = "https://api.fda.gov/drug/event.json"


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
    """Primeiro e último dia do mês (ano, mês)."""
    first = date(y, m, 1)
    # próximo mês: se dezembro, vira jan do próximo ano
    if m == 12:
        next_first = date(y + 1, 1, 1)
    else:
        next_first = date(y, m + 1, 1)
    last = next_first - timedelta(days=1)
    return first, last


def _resolve_window(ctx: Dict[str, Any]) -> Tuple[date, date]:
    """
    Define a janela (primeiro_dia, ultimo_dia).
    Prioridade:
      1) dag_run.conf: {"year": 2023, "month": 5}
      2) dag_run.conf: {"start": "YYYYMMDD", "end": "YYYYMMDD"}  (inclusive)
      3) padrão do @monthly: mês de (data_interval_end - 1 dia)
    """
    conf = (ctx.get("dag_run") or {}).get("conf") or {}

    # 1) year/month
    y = conf.get("year")
    m = conf.get("month")
    if isinstance(y, int) and isinstance(m, int) and 1 <= m <= 12:
        first, last = _month_bounds_from_year_month(y, m)
        return first, last

    # 2) start/end (YYYYMMDD)
    start = conf.get("start")
    end = conf.get("end")
    if isinstance(start, str) and isinstance(end, str) and len(start) == 8 and len(end) == 8:
        try:
            first = datetime.strptime(start, "%Y%m%d").date()
            last = datetime.strptime(end, "%Y%m%d").date()
            if first <= last:
                return first, last
        except Exception:
            pass  # cai no padrão

    # 3) padrão mensal a partir do data_interval_end
    data_interval_end: datetime = ctx["data_interval_end"]
    last = (data_interval_end - timedelta(days=1)).date()
    first = last.replace(day=1)
    return first, last


def _ds(d: date) -> str:
    return d.strftime("%Y%m%d")


# --------------------------- DAG ---------------------------

@dag(
    dag_id=DAG_ID,
    description="Coletar eventos OpenFDA de atorvastatina (Lipitor) por 1 mês e salvar no BigQuery para série histórica",
    start_date=datetime(2023, 1, 1),  # habilita backfill desde 2023
    schedule="@monthly",
    catchup=True,  # para criar runs históricos automaticamente
    default_args={"owner": "data-eng"},
    tags=["openfda", "atorvastatin", "lipitor", "bigquery"],
)
def atorvastatin_openfda_monthly_to_bq():
    @task(task_id="fetch_openfda", retries=2, retry_delay=timedelta(minutes=5))
    def fetch_openfda() -> List[Dict[str, Any]]:
        """
        Fluxo:
        (A) count=receivedate (atorvastatina)
        (B) count=receiptdate (atorvastatina)
        (C) PROBE sem filtro de produto (apenas por data) → detecta lag/cobertura do mês
        (D) Se PROBE > 0, paginação + agregação local com filtro amplo
        Retorna [{"time":"YYYYMMDD","count":N}, ...]
        """
        ctx = get_current_context()
        first_day, last_day = _resolve_window(ctx)
        start_ds, end_ds = _ds(first_day), _ds(last_day)
        logging.info("Janela consultada: %s..%s", start_ds, end_ds)

        # Filtro de produto amplo e tolerante (sem .exact para aumentar recall)
        product_filter = (
            '('
            'patient.drug.medicinalproduct:(atorvastatin OR lipitor) OR '
            'patient.drug.openfda.substance_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.brand_name:(LIPITOR)'
            ')'
        )
        rcv_range = f"receivedate:[{start_ds} TO {end_ds}]"
        rcp_range = f"receiptdate:[{start_ds} TO {end_ds}]"

        # (A) count por receivedate
        params_a = {"search": f"{product_filter} AND {rcv_range}", "count": "receivedate", "limit": 1000}
        data_a = _openfda_get(OPENFDA_ENDPOINT, params_a, timeout=60)
        if data_a and isinstance(data_a.get("results"), list) and data_a["results"]:
            logging.info("count(receivedate) → %d linhas.", len(data_a["results"]))
            return data_a["results"]

        # (B) count por receiptdate
        params_b = {"search": f"{product_filter} AND {rcp_range}", "count": "receiptdate", "limit": 1000}
        data_b = _openfda_get(OPENFDA_ENDPOINT, params_b, timeout=60)
        if data_b and isinstance(data_b.get("results"), list) and data_b["results"]:
            logging.info("count(receiptdate) → %d linhas.", len(data_b["results"]))
            return data_b["results"]

        # (C) PROBE sem filtro de produto (apenas data) para receivedate
        probe = _openfda_get(OPENFDA_ENDPOINT, {"search": rcv_range, "count": "receivedate"}, timeout=45)
        probe_n = len((probe or {}).get("results", []) or [])
        logging.info("[PROBE receivedate] %d pontos.", probe_n)

        # Se probe vazio, tenta probe por receiptdate (alguns meses só têm esse campo)
        if probe_n == 0:
            probe2 = _openfda_get(OPENFDA_ENDPOINT, {"search": rcp_range, "count": "receiptdate"}, timeout=45)
            probe_n2 = len((probe2 or {}).get("results", []) or [])
            logging.info("[PROBE receiptdate] %d pontos.", probe_n2)
            probe_n = probe_n2

        if probe_n == 0:
            msg = (
                f"[PROBE] Nenhum registro FAERS no mês {first_day:%Y-%m} "
                f"(janela {start_ds}..{end_ds}). Provável atraso/cobertura."
            )
            if ALLOW_EMPTY:
                logging.warning(msg)
                return []
            raise RuntimeError(msg)

        # (D) Paginação + agregação local com filtro amplo
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
            page = _openfda_get(OPENFDA_ENDPOINT, params_page, timeout=60)
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
        logging.info("Fallback: agregados %d eventos em %d dias.", total, len(out))
        return out

    @task(task_id="transform_events")
    def transform_events(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normaliza -> receivedate (YYYY-MM-DD) e count (INT)."""
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

        logging.info("Transform → %d linhas.", len(out))
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
        logging.info("BigQuery: carregadas %d linhas em %s.%s.%s", len(rows), project_id, dataset_id, table_id)

    fetched = fetch_openfda()
    transformed = transform_events(fetched)
    save_to_bigquery(transformed)


dag = atorvastatin_openfda_monthly_to_bq()
