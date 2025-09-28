from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


DAG_ID = "openfda_tirzepatide_monthly_to_bq"


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
        Buscar eventos do período escolhido na OpenFDA (aggregação diária por receivedate).
        Corrige intervalos vazios/invertidos pegando sempre o mês do último dia da janela.
        """
        ctx = get_current_context()
        data_interval_end: datetime = ctx["data_interval_end"]

        # Último dia incluído é o dia anterior ao fim da janela
        last_day = (data_interval_end - timedelta(days=1)).date()
        # Primeiro dia é o primeiro dia do mesmo mês do last_day
        first_day = last_day.replace(day=1)

        start_str = first_day.strftime("%Y%m%d")
        end_str = last_day.strftime("%Y%m%d")

        # Se por algum motivo o intervalo ficar invertido, devolve vazio
        if start_str > end_str:
            return []

        endpoint = "https://api.fda.gov/drug/event.json"
        search = (
            'patient.drug.medicinalproduct:("tirzepatide" OR "Mounjaro" OR "Zepbound") '
            f"AND receivedate:[{start_str} TO {end_str}]"
        )
        params = {
            "search": search,
            "count": "receivedate",
            "limit": 1000,
        }
        headers = {
            # Define um User-Agent explícito para evitar bloqueios de alguns proxies/CDNs
            "User-Agent": f"airflow-dag/{DAG_ID}"
        }

        resp = requests.get(endpoint, params=params, headers=headers, timeout=60)

        # A API OpenFDA retorna 404 quando não há resultados para a consulta.
        if resp.status_code == 404:
            return []

        resp.raise_for_status()
        payload = resp.json()

        results = payload.get("results", [])
        if not isinstance(results, list):
            raise ValueError("Resposta inesperada da OpenFDA: campo 'results' não é uma lista.")

        # Ex.: cada item: {"time": "YYYYMMDD", "count": 123}
        return results

    @task(task_id="transform_events")
    def transform_events(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalizar campos e filtrar colunas:
        - Seleciona ["receivedate", "count"]
        - Renomeia 'time' -> 'receivedate' se existir
        - Converte receivedate para YYYY-MM-DD (DATE)
        - Converte count para INT
        """
        out: List[Dict[str, Any]] = []

        for row in results or []:
            received = row.get("receivedate", row.get("time"))
            cnt = row.get("count")

            if received is None or cnt is None:
                continue

            received_str = str(received)
            # Formatos comuns: YYYYMMDD (da OpenFDA) ou ISO
            try:
                if len(received_str) == 8 and received_str.isdigit():
                    dt = datetime.strptime(received_str, "%Y%m%d").date()
                else:
                    dt = datetime.fromisoformat(received_str).date()
            except Exception:
                continue

            try:
                cnt_int = int(cnt)
            except (TypeError, ValueError):
                continue

            out.append({"receivedate": dt.isoformat(), "count": cnt_int})

        return out

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(rows: List[Dict[str, Any]]) -> None:
        """
        Gravar no BigQuery no dataset dataset_fda, tabela particionada por dia.
        """
        if not rows:
            return

        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_tirzepatide_daily"
        gcp_conn_id = "google_cloud_default"

        # Cliente autenticado via conexão do Airflow
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
        client: bigquery.Client = bq_hook.get_client(project_id=project_id)

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

        # Cria dataset se necessário
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

        table_ref = dataset_ref.table(table_id)

        schema = [
            bigquery.SchemaField("receivedate", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("count", "INT64", mode="REQUIRED"),
        ]

        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="receivedate",
        )

        # Garante existência da tabela com particionamento
        try:
            client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = time_partitioning
            client.create_table(table)

        # Carrega dados (append) como JSON Lines em memória
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        import json
        from io import BytesIO

        buffer = BytesIO()
        for r in rows:
            buffer.write((json.dumps(r) + "\n").encode("utf-8"))
        buffer.seek(0)

        load_job = client.load_table_from_file(buffer, table_ref, job_config=job_config)
        load_job.result()

    # Orquestração: fetch_openfda >> transform_events >> save_to_bigquery
    fetched = fetch_openfda()
    transformed = transform_events(fetched)
    save_to_bigquery(transformed)


dag = tirzepatide_openfda_monthly_to_bq()
