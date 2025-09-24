# dags/bitcoin_etl_coingecko1.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import timedelta
import pendulum
import requests
import pandas as pd


DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "Alex Lopes,Open in Cloud IDE",
}


@task
def fetch_bitcoin_history_from_coingecko1():
    """
    Coleta dados horários do Bitcoin na janela "ontem"
    usando CoinGecko /coins/bitcoin/market_chart/range.
    """
    ctx = get_current_context()

    # Janela "ontem": [data_interval_start - 1 dia, data_interval_start)
    end_time = ctx["data_interval_start"]
    start_time = end_time - timedelta(days=1)

    print(f"[UTC] janela-alvo: {start_time} -> {end_time}")

    # CoinGecko exige epoch em segundos (não ms)
    start_s = int(start_time.timestamp())
    end_s = int(end_time.timestamp())

    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_s,
        "to": end_s,
    }

    # Observação: CoinGecko pode aplicar rate limit (HTTP 429).
    # O retry geral é tratado pelo Airflow (default_args['retries']).
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    # payload contém listas de pares [timestamp_ms, valor]
    prices = payload.get("prices", [])
    caps = payload.get("market_caps", [])
    vols = payload.get("total_volumes", [])

    if not prices:
        print("Sem dados retornados pela API para a janela especificada.")
        return

    # Constrói DataFrames individuais
    df_p = pd.DataFrame(prices, columns=["time_ms", "price_usd"])
    df_c = pd.DataFrame(caps, columns=["time_ms", "market_cap_usd"])
    df_v = pd.DataFrame(vols, columns=["time_ms", "volume_usd"])

    # Merge por timestamp (ms)
    df = df_p.merge(df_c, on="time_ms", how="outer").merge(df_v, on="time_ms", how="outer")

    # Converte timestamp e organiza índice
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df.drop(columns=["time_ms"], inplace=True)
    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)

    # Preview no log
    print("Prévia horária (top 10):")
    print(df.head(10).to_string())

   
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="postgres")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql("bitcoin_history_renata_rrta", con=engine, if_exists="append", index=True)


@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # diário à 00:00 UTC
    start_date=pendulum.datetime(2025, 9, 2, tz="UTC"),
    catchup=True,
    max_active_runs=1,       # <-- only one DagRun at a time
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm3webulw15k701npm2uhu77t/cloud-ide/cm42rbvn10lqk01nlco70l0b8/cm44gkosq0tof01mxajutk86g",
    },
    tags=["bitcoin", "etl", "coingecko"],
)
def bitcoin_etl_coingecko1():
    fetch_bitcoin_history_from_coingecko1()


# Airflow descobre qualquer variável global que referencie um DAG
dag = bitcoin_etl_coingecko1()
