from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import Counter

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


DAG_ID = "openfda_tirzepatide_monthly_to_bq"


def _openfda_get(
    endpoint: str, params: Dict[str, Any], timeout: int = 60
) -> Optional[Dict[str, Any]]:
    """Wrapper para GET da OpenFDA: trata 404 como vazio e valida JSON."""
    headers = {"User-Agent": f"airflow-dag/{DAG_ID}"}
    resp = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return None


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
    @task(task_id="fetch_openfda", retries=2, retry_delay=timedelta(minutes=5)))
    def fetch_openfda() -> List[Dict[str, Any]]:
        """
        Busca agregada diária (count=receivedate). Se vier vazio, faz fallback
        paginando eventos e agregando localmente.
        """
        ctx = get_current_context()
        data_interval_end: datetime = ctx["data_interval_end"]

        # Janela mensal (primeiro ao último dia do mês da execução)
        last_day = (data_interval_end - timedelta(days=1)).date()
        first_day = last_day.replace(day=1)

        start_ds = first_day.strftime("%Y%m%d")
        end_ds = last_day.strftime("%Y%m%d")

        if start_ds > end_ds:
            return []

        endpoint = "https://api.fda.gov/drug/event.json"

        # Filtro robusto: combina medicinalproduct, brand_name e substance_name,
        # usando .exact onde apropriado para reduzir tokenização.
        product_filter = (
            '('
            'patient.drug.medicinalproduct:("tirzepatide" OR "Mounjaro" OR "Zepbound") OR '
            'patient.drug.medicinalproduct.exact:("TIRZEPATIDE" OR "MOUNJARO" OR "ZEPBOUND") OR '
            'patient.drug.openfda.brand_name.exact:("MOUNJARO" OR "ZEPBOUND") OR '
            'patient.drug.openfda.substance_name.exact:("TIRZEPATIDE")'
            ')'
        )

        date_filter = f"receivedate:[{start_ds} TO {end_ds}]"
        base_search = f"{product_filter} AND {date_filter}"

        # 1) Tentativa com agregação nativa
        params_count = {
            "search": base_search,
            "count": "receivedate",
            "limit": 1000,
        }
        data = _openfda_get(endpoint, params_count, timeout=60)
        if data and isinstance(data.get("results"), list) and data["results"]:
            # Resultados vêm como [{"time":"YYYYMMDD","count":N}, ...]
            return data["results"]

        # 2) Fallback: paginação de eventos e agregação local por receivedate
        # Usamos somente o campo receivedate para minimizar payload.
        rows_per_page = 100
        skip = 0
        counts = Counter()

        while True:
            params_page = {
                "search": base_search + " AND _exists_:receivedate",
                "limit": rows_per_page,
                "skip": skip,
                "sort": "receivedate:asc",
                "fields": "receivedate",
            }
            page = _openfda_get(endpoint, params_page, timeout=60)
            results = (page or {}).get("results", [])
            if not results:
                break

            for r in results:
                rd = r.get("receivedate")
                if rd:
                    counts[str(rd)] += 1

            if len(results) < rows_per_page:
                break
            skip += rows_per_page

        # Converte o Counter para o formato esperado de saída
        out = [{"time": k, "count": v} for k, v in sorted(counts.items())]
        return out

    @task(task_id="transform_events")
    def transform_events(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normaliza:
        - 'time' -> 'receivedate'
        - receivedate -> YYYY-MM-DD
        - count -> INT
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
        return out

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(rows: List[Dict[str, Any]]) -> None:
        if not rows:
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
            # Location padrão do sandbox é US; ajuste se necessário.
            ds = bigquery.Dataset(dataset_ref)
            ds.location = "US"
            client.create_dataset(ds, exists_ok=True)

        table_ref = dataset_ref.table(table_id)

        schema = [
            bigquery.SchemaField("receivedate", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("count", "INT64", mode="REQUIRED"),
        ]
        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="receivedate",
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

    fetched = fetch_openfda()
    transformed = transform_events(fetched)
    save_to_bigquery(transformed)


dag = tirzepatide_openfda_monthly_to_bq()
