# -*- coding: utf-8 -*-
"""
DAG: fda_atorvastatin_daily
Objetivo: popular a tabela diária particionada sem apagar histórico quando não houver dados.
Fonte: dataset_fda.drug_events_atorvastatin_sample (linhas filtradas para ATORVASTATIN)
Alvo:  dataset_fda.drug_events_atorvastatin_daily (particionada por receiptdate)

Estratégia:
- gate_has_data consulta a sample para o dia de execução; se não houver linhas, pula o restante.
- merge_daily cria a tabela (se não existir) e faz MERGE idempotente do dia.

Requisitos:
- Airflow >= 2
- Providers Google: apache-airflow-providers-google
- Conexão GCP: "google_cloud_default" (ajuste se usar outra)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# Parâmetros do ambiente
# =========================
PROJECT_ID = "bigquery-sandbox-471123"
DATASET = "dataset_fda"
LOCATION = "US"  # seu projeto está em region-us
GCP_CONN_ID = "google_cloud_default"

TABLE_SAMPLE = f"{PROJECT_ID}.{DATASET}.drug_events_atorvastatin_sample"
TABLE_DAILY = f"{PROJECT_ID}.{DATASET}.drug_events_atorvastatin_daily"

# =========================
# Funções auxiliares
# =========================
def has_rows_for_ds(ds: str, **_) -> bool:
    """
    Retorna True se existir ao menos 1 linha na SAMPLE para o receiptdate = {{ ds }}.
    ds chega no formato 'YYYY-MM-DD'; convertemos para 'YYYYMMDD'.
    """
    ds_nodash = ds.replace("-", "")
    sql = f"""
    SELECT COUNT(1) > 0 AS has_rows
    FROM `{TABLE_SAMPLE}`
    WHERE receiptdate = '{ds_nodash}'
    """
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False, location=LOCATION)
    records = hook.get_records(sql)
    return bool(records and records[0][0])

# =========================
# SQL (templated) do MERGE
# =========================
# Observação:
# - CREATE TABLE IF NOT EXISTS + PARTITION BY receiptdate garante tabela particionada.
# - MERGE atualiza/insere a partição do dia; não mexe nas demais partições.
# - A sample já contém apenas eventos de atorvastatina (conforme seu pipeline).
SQL_MERGE = f"""
-- cria a tabela particionada se ainda não existir
CREATE TABLE IF NOT EXISTS `{TABLE_DAILY}`
(
  receiptdate DATE,
  events      INT64,
  ingested_at TIMESTAMP
)
PARTITION BY receiptdate
OPTIONS(
  description = 'Contagem diária de eventos FAERS/atorvastatina'
);

-- upsert do dia de execução
MERGE `{TABLE_DAILY}` D
USING (
  SELECT
    DATE(PARSE_DATE('%Y%m%d', receiptdate)) AS receiptdate,
    COUNT(*) AS events
  FROM `{TABLE_SAMPLE}`
  WHERE receiptdate = FORMAT_DATE('%Y%m%d', DATE('{{ ds }}'))
  GROUP BY 1
) S
ON D.receiptdate = S.receiptdate
WHEN MATCHED THEN
  UPDATE SET
    events = S.events,
    ingested_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (receiptdate, events, ingested_at)
  VALUES (S.receiptdate, S.events, CURRENT_TIMESTAMP());
"""

# =========================
# Definição da DAG
# =========================
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fda_atorvastatin_daily",
    description="Gera tabela diária de eventos de atorvastatina via MERGE, evitando truncar histórico.",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args=default_args,
    tags=["fda", "bigquery", "atorvastatin"],
) as dag:

    # 1) Gate: só segue se houver dados para o dia
    gate_has_data = ShortCircuitOperator(
        task_id="gate_has_data",
        python_callable=has_rows_for_ds,
        op_kwargs={"ds": "{{ ds }}"},  # injeta o ds renderizado
    )

    # 2) MERGE diário idempotente e seguro (partição do dia)
    merge_daily = BigQueryInsertJobOperator(
        task_id="merge_daily",
        configuration={
            "query": {
                "query": SQL_MERGE,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
    )

    gate_has_data >> merge_daily
