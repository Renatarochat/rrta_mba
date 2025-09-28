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


# Config
DAG_ID = "openfda_atorvastatin_monthly_to_bq"
OPENFDA_ENDPOINT = "https://api.fda.gov/drug/event.json"
PROJECT_ID = "bigquery-sandbox-471123"
DATASET_ID = "dataset_fda"
TABLE_DAILY = "drug_events_atorvastatin_daily"
TABLE_PROBE = "drug_events_atorvastatin_probe"
TABLE_SAMPLE = "drug_events_atorvastatin_sample"
GCP_CONN_ID = "google_cloud_default"


# --------------------------- Helpers ---------------------------

def _month_bounds(ctx: Dict[str, Any]) -> Tuple[date, date]:
    """
    Janela (primeiro_dia, ultimo_dia).
      1) dag_run.conf: {"year": 2023, "month": 4}
      2) mês de (data_interval_end - 1 dia)
    """
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
        try:
            resp = requests.get(OPENFDA_ENDPOINT, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 404:
                logging.info("API retornou 404 para params: %s", params)
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                logging.warning("API retornou %d. Tentativa %d de 3. Re-tentando em %d segundos.", resp.status_code, attempt + 1, 2 * (attempt + 1))
                time.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception as e:
                logging.error("Erro ao decodificar JSON da resposta da API: %s. Resposta: %s", e, resp.text)
                return None
        except requests.exceptions.Timeout:
            logging.error("Timeout ao chamar a API OpenFDA com params: %s", params)
            time.sleep(2 * (attempt + 1))
            continue
        except requests.exceptions.RequestException as e:
            logging.error("Erro na requisição à API OpenFDA com params %s: %s", params, e)
            raise
    logging.error("Falha após 3 tentativas de chamar a API OpenFDA com params: %s", params)
    return None


def _bq_client() -> bigquery.Client:
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    return hook.get_client(project_id=PROJECT_ID)


def _bq_ensure() -> bigquery.Client:
    client = _bq_client()
    ds_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    try:
        client.get_dataset(ds_ref)
        logging.info("Dataset %s já existe.", DATASET_ID)
    except Exception as e:
        logging.info("Dataset %s não existe. A criar...", DATASET_ID)
        ds = bigquery.Dataset(ds_ref)
        ds.location = "US"
        try:
            client.create_dataset(ds, exists_ok=True)
            logging.info("Dataset %s criado com sucesso.", DATASET_ID)
        except Exception as create_e:
            logging.error("Erro ao criar dataset %s: %s", DATASET_ID, create_e)
            raise # Re-raise para falhar a tarefa se a criação do dataset falhar
    return client


# --------------------------- DAG ---------------------------

@dag(
    dag_id=DAG_ID,
    description="Série diária de eventos OpenFDA para Atorvastatina (Lipitor). "
                "Salva contagens diárias + probe + amostra crua.",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=True,
    default_args={"owner": "data-eng"},
    tags=["openfda", "atorvastatin", "lipitor", "bigquery"],
)
def atorvastatin_openfda_monthly_to_bq():
    @task(task_id="fetch_openfda", retries=2, retry_delay=timedelta(minutes=5))
    def fetch_openfda() -> Dict[str, Any]:
        """
        Coleta:
          - counts: agregação diária (primeiro por receiptdate, fallback para receivedate,
            e, se ainda vier vazio, agrega a partir de eventos crus).
          - probe: telemetria do mês (pts por receipt/received sem filtro de produto).
          - sample: até 200 eventos crus para diagnóstico.

        Retorno:
        {
          "counts": [{"receivedate":"YYYY-MM-DD","count":N}, ...],
          "probe": {...},
          "sample": [ {...}, ... ]
        }
        """
        ctx = get_current_context()
        first_day, last_day = _month_bounds(ctx)
        start_ds, end_ds = _ds(first_day), _ds(last_day)
        logging.info("Janela consultada: %s..%s", start_ds, end_ds)

        # Filtro amplo (maior recall; case-insensitive)
        product_filter = (
            '('
            'patient.drug.medicinalproduct:(atorvastatin OR "ATORVASTATIN" OR lipitor) OR '
            'patient.drug.activesubstance.activesubstancename:(ATORVASTATIN) OR '
            'patient.drug.openfda.substance_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.generic_name:(ATORVASTATIN) OR '
            'patient.drug.openfda.brand_name:(LIPITOR)'
            ')'
        )

        # ---------- A) counts por receiptdate ----------
        counts_out: List[Dict[str, Any]] = []
        params_recpt = {
            "search": f"{product_filter} AND receiptdate:[{start_ds} TO {end_ds}]",
            "count": "receiptdate",
            "limit": 1000,
        }
        logging.info("A tentar obter contagens por receiptdate com params: %s", params_recpt)
        data = _openfda_get(params_recpt, timeout=60)
        logging.info("Resposta da API (receiptdate): %s", data)
        results = (data or {}).get("results") or []
        logging.info("Contagens iniciais (receiptdate) encontradas: %d", len(results))

        # ---------- B) fallback por receivedate ----------
        if not results:
            logging.info("Nenhuma contagem por receiptdate. A tentar fallback por receivedate.")
            params_recv = {
                "search": f"{product_filter} AND receivedate:[{start_ds} TO {end_ds}]",
                "count": "receivedate",
                "limit": 1000,
            }
            logging.info("A tentar obter contagens por receivedate com params: %s", params_recv)
            data2 = _openfda_get(params_recv, timeout=60)
            logging.info("Resposta da API (receivedate): %s", data2)
            results = (data2 or {}).get("results") or []
            logging.info("Contagens de fallback (receivedate) encontradas: %d", len(results))

        # Normaliza contagens vindas da API (campo "time")
        for row in results:
            t = row.get("time")
            c = row.get("count")
            if not t or c is None:
                logging.warning("Linha de contagem ignorada devido a 'time' ou 'count' ausente: %s", row)
                continue
            s = str(t)
            try:
                dt = datetime.strptime(s, "%Y%m%d").date() if len(s) == 8 and s.isdigit() else datetime.fromisoformat(s).date()
                counts_out.append({"receivedate": dt.isoformat(), "count": int(c)})
            except Exception as e:
                logging.error("Erro ao analisar data '%s' na linha %s: %s", s, row, e)
                continue
        logging.info("Contagens normalizadas após API: %d", len(counts_out))

        # ---------- C) se ainda não temos counts, agrega eventos crus ----------
        if not counts_out:
            logging.info("Nenhuma contagem após normalização. A tentar agregar eventos crus.")
            window = f"(receiptdate:[{start_ds} TO {end_ds}] OR receivedate:[{start_ds} TO {end_ds}])"
            search = f"{product_filter} AND {window}"
            limit = 100
            skip = 0
            raw: List[Dict[str, Any]] = []
            while True:
                page = _openfda_get(
                    {"search": search, "limit": limit, "skip": skip, "fields": "receiptdate,receivedate", "sort": "receiptdate:asc"},
                    timeout=60,
                )
                batch = (page or {}).get("results", [])
                if not batch:
                    logging.info("Fim da paginação ou nenhum evento bruto encontrado.")
                    break
                raw.extend(batch)
                if len(batch) < limit:
                    logging.info("Último lote de eventos brutos com menos de %d itens.", limit)
                    break
                skip += limit
                logging.info("Eventos brutos coletados até agora: %d", len(raw))

            agg: Counter[str] = Counter()
            for ev in raw:
                s = str(ev.get("receiptdate") or ev.get("receivedate") or "")
                if not s:
                    logging.warning("Evento bruto ignorado devido a 'receiptdate'/'receivedate' ausente: %s", ev)
                    continue
                try:
                    d = datetime.strptime(s, "%Y%m%d").date() if len(s) == 8 and s.isdigit() else datetime.fromisoformat(s).date()
                except Exception as e:
                    logging.error("Erro ao analisar data '%s' em evento bruto %s: %s", s, ev, e)
                    continue
                agg[d.isoformat()] += 1
            counts_out = [{"receivedate": d, "count": int(n)} for d, n in sorted(agg.items())]
            logging.info("Contagens agregadas de eventos brutos: %d", len(counts_out))

        # ---------- D) Probe (sem filtro de produto) ----------
        probe_recv = _openfda_get({"search": f"receivedate:[{start_ds} TO {end_ds}]", "count": "receivedate"}, timeout=30)
        recv_pts = len((probe_recv or {}).get("results", []) or [])
        probe_rcpt = _openfda_get({"search": f"receiptdate:[{start_ds} TO {end_ds}]", "count": "receiptdate"}, timeout=30)
        rcpt_pts = len((probe_rcpt or {}).get("results", []) or [])
        has_data = (recv_pts + rcpt_pts) > 0

        # ---------- E) Sample com filtro de produto (até 200) ----------
        sample_rows: List[Dict[str, Any]] = []
        limit = 100
        skip = 0
        while len(sample_rows) < 200:
            page = _openfda_get(
                {
                    "search": f"{product_filter} AND (receiptdate:[{start_ds} TO {end_ds}] OR receivedate:[{start_ds} TO {end_ds}])",
                    "limit": limit,
                    "skip": skip,
                    "fields": "receiptdate,receivedate,patient.drug,summary,reaction",
                },
                timeout=60,
            )
            batch = (page or {}).get("results", [])
            if not batch:
                break
            sample_rows.extend(batch)
            if len(batch) < limit:
                break
            skip += limit

        logging.info("counts=%d, probe(receipt=%d, received=%d), sample=%d",
                     len(counts_out), rcpt_pts, recv_pts, len(sample_rows))

        return {
            "counts": counts_out,
            "probe": {
                "window_start": start_ds,
                "window_end": end_ds,
                "has_data": has_data,
                "received_pts": recv_pts,
                "receipt_pts": rcpt_pts,
            },
            "sample": sample_rows[:200],
        }

    @task(task_id="save_probe_bq")
    def save_probe_bq(payload: Dict[str, Any]) -> None:
        client = _bq_ensure()
        table_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID).table(TABLE_PROBE)
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
            logging.info("Tabela %s já existe.", TABLE_PROBE)
        except Exception:
            logging.info("Tabela %s não existe. A criar...", TABLE_PROBE)
            client.create_table(bigquery.Table(table_ref, schema=schema))
            logging.info("Tabela %s criada com sucesso.", TABLE_PROBE)

        pr = payload.get("probe") or {}
        row = {
            "window_start": pr.get("window_start", ""),
            "window_end": pr.get("window_end", ""),
            "has_data": bool(pr.get("has_data", False)),
            "received_pts": int(pr.get("received_pts", 0)),
            "receipt_pts": int(pr.get("receipt_pts", 0)),
            "counts_points": int(len(payload.get("counts") or [])),
            "sample_rows": int(len(payload.get("sample") or [])),
            "ingested_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }

        from io import BytesIO
        import json
        buf = BytesIO((json.dumps(row) + "\n").encode("utf-8"))
        try:
            job = client.load_table_from_file(
                buf,
                table_ref,
                job_config=bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                ),
            )
            job.result()
            logging.info("Carregada 1 linha em %s.%s.%s", PROJECT_ID, DATASET_ID, TABLE_PROBE)
        except Exception as load_e:
            logging.error("Erro ao carregar dados para %s.%s.%s: %s", PROJECT_ID, DATASET_ID, TABLE_PROBE, load_e)
            raise

    @task(task_id="save_sample_bq")
    def save_sample_bq(payload: Dict[str, Any]) -> None:
        sample = payload.get("sample") or []
        if not sample:
            logging.info("Sem amostras para salvar.")
            return
        client = _bq_ensure()
        table_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID).table(TABLE_SAMPLE)
        schema = [
            bigquery.SchemaField("raw_event", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("receivedate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("receiptdate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        try:
            client.get_table(table_ref)
            logging.info("Tabela %s já existe.", TABLE_SAMPLE)
        except Exception:
            logging.info("Tabela %s não existe. A criar...", TABLE_SAMPLE)
            client.create_table(bigquery.Table(table_ref, schema=schema))
            logging.info("Tabela %s criada com sucesso.", TABLE_SAMPLE)

        from io import BytesIO
        import json
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        buf = BytesIO()
        for ev in sample:
            try:
                buf.write((json.dumps({
                    "raw_event": json.dumps(ev, ensure_ascii=False),
                    "receivedate": ev.get("receivedate"),
                    "receiptdate": ev.get("receiptdate"),
                    "ingested_at": now,
                }) + "\n").encode("utf-8"))
            except Exception as data_e:
                logging.error("Erro ao processar evento de amostra: %s. Evento ignorado: %s", data_e, ev)
        buf.seek(0)

        if buf.getbuffer().nbytes == 0:
            logging.warning("Buffer de dados de amostra vazio após filtragem/validação. Nenhum dado será carregado.")
            return

        try:
            job = client.load_table_from_file(
                buf,
                table_ref,
                job_config=bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                ),
            )
            job.result()
            logging.info("Carregadas %d linhas em %s.%s.%s", len(sample), PROJECT_ID, DATASET_ID, TABLE_SAMPLE)
        except Exception as load_e:
            logging.error("Erro ao carregar dados para %s.%s.%s: %s", PROJECT_ID, DATASET_ID, TABLE_SAMPLE, load_e)
            raise

    @task(task_id="save_daily_bq")
    def save_daily_bq(payload: Dict[str, Any]) -> None:
        rows = payload.get("counts") or []
        logging.info("save_daily_bq recebeu %d linhas para carregar.", len(rows))
        client = _bq_ensure()

        ds_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
        table_ref = ds_ref.table(TABLE_DAILY)
        schema = [
            bigquery.SchemaField("receivedate", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("count", "INT64", mode="REQUIRED"),
        ]
        time_part = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="receivedate")

        try:
            client.get_table(table_ref)
            logging.info("Tabela %s.%s.%s já existe.", PROJECT_ID, DATASET_ID, TABLE_DAILY)
        except Exception as e:
            logging.info("Tabela %s.%s.%s não existe. A criar...", PROJECT_ID, DATASET_ID, TABLE_DAILY)
            tbl = bigquery.Table(table_ref, schema=schema)
            tbl.time_partitioning = time_part
            try:
                client.create_table(tbl)
                logging.info("Tabela %s.%s.%s criada com sucesso.", PROJECT_ID, DATASET_ID, TABLE_DAILY)
            except Exception as create_e:
                logging.error("Erro ao criar tabela %s.%s.%s: %s", PROJECT_ID, DATASET_ID, TABLE_DAILY, create_e)
                raise # Re-raise para falhar a tarefa se a criação da tabela falhar

        if not rows:
            logging.info("Nenhum dado agregado para carregar na tabela diária. A sair.")
            return

        from io import BytesIO
        import json
        buf = BytesIO()
        processed_rows_count = 0
        for r in rows:
            try:
                # Validação adicional para garantir que os dados correspondem ao esquema
                # Por exemplo, garantir que 'receivedate' é uma string de data válida
                datetime.fromisoformat(r["receivedate"])
                buf.write((json.dumps(r) + "\n").encode("utf-8"))
                processed_rows_count += 1
            except KeyError as ke:
                logging.error("Campo ausente no registo: %s. Registo ignorado: %s", ke, r)
            except ValueError as ve:
                logging.error("Formato de data inválido em 'receivedate': %s. Registo ignorado: %s", ve, r)
            except Exception as data_e:
                logging.error("Erro inesperado ao processar registo: %s. Registo ignorado: %s", data_e, r)

        buf.seek(0)
        if buf.getbuffer().nbytes == 0:
            logging.warning("Buffer de dados vazio após filtragem/validação. Nenhum dado será carregado.")
            return

        try:
            job = client.load_table_from_file(
                buf,
                table_ref,
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                ),
            )
            job.result() # Espera a conclusão do job e verifica erros
            logging.info("Carregadas %d linhas em %s.%s.%s", processed_rows_count, PROJECT_ID, DATASET_ID, TABLE_DAILY)
        except Exception as load_e:
            logging.error("Erro ao carregar dados para %s.%s.%s: %s", PROJECT_ID, DATASET_ID, TABLE_DAILY, load_e)
            raise # Re-raise para falhar a tarefa se o carregamento falhar

    # Orquestração
    payload = fetch_openfda()
    save_daily_bq(payload)
    save_probe_bq(payload)
    save_sample_bq(payload)


dag = atorvastatin_openfda_monthly_to_bq()
