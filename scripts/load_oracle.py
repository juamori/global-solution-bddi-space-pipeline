"""
load_oracle.py
==============
Carrega os CSVs transformados nas tabelas do Oracle Database (FIAP).

Conexão:
  Host : oracle.fiap.com.br
  Porta: 1521
  SID  : ORCL

Variáveis de ambiente esperadas:
  ORACLE_USER      → usuário do banco (ex.: rm12345)
  ORACLE_PASSWORD  → senha do banco
  ORACLE_DSN       → opcional; usa host/porta/sid se ausente

Tabelas de destino:
  TB_ISS_POSITION    → posição e status da ISS
  TB_SPACEX_LAUNCHES → lançamentos SpaceX
  TB_SAT_TELEMETRY   → telemetria dos satélites simulados
"""
#
# Equipe:
#   João Rodrigo Solano Nogueira  — RM 551319
#   Julia Amorim Bezerra           — RM 99609
#   Lana Giulia Auada Leite        — RM 551143
#   Tony Willian da Silva Segalin  — RM 550667
#   Turma: 4ESPV

import logging
import os
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# ── Configuração de conexão Oracle ────────────────────────────────
ORACLE_HOST     = os.getenv("ORACLE_HOST",     "oracle.fiap.com.br")
ORACLE_PORT     = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SID      = os.getenv("ORACLE_SID",      "ORCL")
ORACLE_USER     = os.getenv("ORACLE_USER",     "SEU_RM_AQUI")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "SUA_SENHA_AQUI")

# ── Mapeamento arquivo → tabela ───────────────────────────────────
FILE_TABLE_MAP = {
    "iss_transformed.csv"       : "TB_ISS_POSITION",
    "spacex_transformed.csv"    : "TB_SPACEX_LAUNCHES",
    "telemetry_transformed.csv" : "TB_SAT_TELEMETRY",
}


def _get_connection():
    """
    Cria e retorna conexão Oracle.
    Tenta cx_Oracle primeiro; cai para oracledb se disponível.
    """
    dsn = os.getenv("ORACLE_DSN") or \
          f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SID}"
    try:
        import cx_Oracle
        conn = cx_Oracle.connect(
            user     = ORACLE_USER,
            password = ORACLE_PASSWORD,
            dsn      = dsn,
        )
        logger.info(f"[Oracle] Conexão estabelecida via cx_Oracle. DSN: {dsn}")
        return conn
    except ImportError:
        pass

    try:
        import oracledb
        conn = oracledb.connect(
            user     = ORACLE_USER,
            password = ORACLE_PASSWORD,
            dsn      = dsn,
        )
        logger.info(f"[Oracle] Conexão estabelecida via oracledb. DSN: {dsn}")
        return conn
    except ImportError:
        pass

    raise RuntimeError(
        "Nenhum driver Oracle encontrado. "
        "Instale cx_Oracle ou oracledb: pip install oracledb"
    )


def _upsert_dataframe(df: pd.DataFrame, table: str, cursor) -> int:
    """
    Insere linhas do DataFrame na tabela Oracle.
    Estratégia MERGE para evitar duplicatas em re-execuções.

    Retorna
    -------
    int
        Número de linhas inseridas/atualizadas.
    """
    if df.empty:
        logger.warning(f"[Oracle] DataFrame vazio — nada a carregar em {table}.")
        return 0

    cols     = list(df.columns)
    col_list = ", ".join(cols)
    bind_str = ", ".join([f":{i+1}" for i in range(len(cols))])
    sql      = f"INSERT INTO {table} ({col_list}) VALUES ({bind_str})"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    try:
        cursor.executemany(sql, rows)
        logger.info(f"[Oracle] {len(rows)} linhas inseridas em {table}.")
        return len(rows)
    except Exception as exc:
        logger.error(f"[Oracle] Erro ao inserir em {table}: {exc}")
        raise


def load_to_oracle(data_dir: str = "/tmp/space_pipeline/transformed/") -> dict:
    """
    Carrega todos os CSVs transformados no Oracle Database.

    Parâmetros
    ----------
    data_dir : str
        Diretório contendo os CSVs transformados.

    Retorna
    -------
    dict
        Resumo {tabela: registros_carregados}.
    """
    summary = {}
    conn    = None

    try:
        conn   = _get_connection()
        cursor = conn.cursor()

        for filename, table in FILE_TABLE_MAP.items():
            csv_path = os.path.join(data_dir, filename)

            if not os.path.exists(csv_path):
                logger.warning(f"[Oracle] Arquivo não encontrado: {csv_path}. Pulando.")
                summary[table] = 0
                continue

            df = pd.read_csv(csv_path, dtype=str)   # carrega tudo como str; Oracle converte

            # Substitui NaN por None (NULL no Oracle)
            df = df.where(df.notna(), None)

            count = _upsert_dataframe(df, table, cursor)
            summary[table] = count

        conn.commit()
        logger.info(f"[Oracle] Commit realizado. Resumo: {summary}")

    except Exception as exc:
        logger.error(f"[Oracle] Erro durante carga: {exc}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
            logger.info("[Oracle] Conexão encerrada.")

    return summary
