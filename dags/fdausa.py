from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from airflow.decorators import dag, task
from airflow.utils.context import get_current_context

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
        """
        ctx = get_current_context()
        data_interval_start: datetime = ctx["data_interval_start"]
        data_interval_end: datetime = ctx["data_interval_end"]

        # A janela do @monthly é [start, end), então subtraímos 1 dia do end
        # para incluir o último dia completo do mês.
        end_minus_1 = data_interval_end - timedelta(days=1)

        start_str = data_interval_start.strftime("%Y%m%d")
        end_str = end_minus_1.strftime("%Y%m%d")

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

        resp = requests.get(endpoint, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        results = payload.get("results", [])
        if not isinstance(results, list):
            raise ValueError("Resposta inesperada da OpenFDA: campo 'results' não é uma lista.")

        return results  # cada item típico: {"time": "YYYYMMDD", "count": 123}

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
            # Renomear se vier como 'time'
            received = row.get("receivedate", row.get("time"))
            cnt = row.get("count")

            if received is None or cnt is None:
                # ignora linhas inválidas
                continue

            # received pode vir como 'YYYYMMDD' ou 'YYYY-MM-DD'
            received_str = str(received)
            if len(received_str) == 8 and received_str.isdigit():
                # YYYYMMDD -> YYYY-MM-DD
                dt = datetime.strptime(received_str, "%Y%m%d").date()
                received_fmt = dt.isoformat()
            else:
                # tenta parse genérico e normaliza
                try:
                    dt = datetime.strptime(received_str, "%Y-%m-%d").date()
                    received_fmt = dt.isoformat()
                except ValueError:
                    # última tentativa com vários formatos comuns
                    try:
                        dt = datetime.fromisoformat(received_str).date()
                        received_fmt = dt.isoformat()
                    except Exception:
                        # se não conseguir converter, pula
                        continue

            try:
                cnt_int = int(cnt)
            except (TypeError, ValueError):
                continue

            out.append({"receivedate": received_fmt, "count": cnt_int})

        return out

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(rows: List[Dict[str, Any]]) -> None:
        """
        Gravar no BigQuery:
        - project_id: bigquery-sandbox-471123
        - dataset: dataset_fda
        - table: drug_events_tirzepatide_daily
        - write_disposition: append
        - partitioning: field=receivedate, type=DAY
        - gcp_conn_id: google_cloud_default
        """
        if not rows:
            # Nada para gravar neste período.
            return

        project_id = "bigquery-sandbox-471123"
        dataset_id = "dataset_fda"
        table_id = "drug_events_tirzepatide_daily"
        gcp_conn_id = "google_cloud_default"

        # Hook para obter o cliente autenticado conforme a conexão do Airflow
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
        client: bigquery.Client = bq_hook.get_client(project_id=project_id)

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

        # Cria o dataset se não existir
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

        table_ref = dataset_ref.table(table_id)

        # Define schema e particionamento
        schema = [
            bigquery.SchemaField("receivedate", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("count", "INT64", mode="REQUIRED"),
        ]

        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="receivedate",
        )

        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = time_partitioning

        # Garante que a tabela existe com o particionamento adequado
        try:
            existing = client.get_table(table_ref)
            # Opcional: validar schema/partitioning; se não bater, mantemos a existente.
            _ = existing  # noop
        except Exception:
            client.create_table(table)

        # Carrega dados (append)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        # Envia como JSON Lines via in-memory
        import json
        from io import BytesIO

        buffer = BytesIO()
        for r in rows:
            buffer.write((json.dumps(r) + "\n").encode("utf-8"))
        buffer.seek(0)

        load_job = client.load_table_from_file(
            buffer, table_ref, job_config=job_config
        )
        load_job.result()  # aguarda o término

    # Orquestração: fetch_openfda >> transform_events >> save_to_bigquery
    fetched = fetch_openfda()
    transformed = transform_events(fetched)
    save_to_bigquery(transformed)


dag = tirzepatide_openfda_monthly_to_bq()
