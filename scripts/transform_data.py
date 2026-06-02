"""
transform_data.py
=================
Transforma e normaliza dados brutos de todas as fontes do pipeline.

Etapas por fonte:
  ISS        → valida coordenadas, converte timestamp Unix, adiciona metadados
  SpaceX     → filtra registros incompletos, normaliza datas, trata nulos
  Telemetria → remove outliers físicos, padroniza status, cria flag de criticidade

Saída:
  Arquivos CSV separados em output_dir/, prontos para carga no Oracle.
"""
#
# Equipe:
#   João Rodrigo Solano Nogueira  — RM 551319
#   Julia Amorim Bezerra           — RM 99609
#   Lana Giulia Auada Leite        — RM 551143
#   Tony Willian da Silva Segalin  — RM 550667
#   Turma: 4ESPV

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
#  ISS
# ══════════════════════════════════════════════════════════════════

def _transform_iss(raw_path: str, out_dir: str) -> str:
    """Transforma registro único da ISS em DataFrame de 1 linha."""
    logger.info("[Transform] Processando dados ISS...")

    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)

    # ── Validações básicas ────────────────────────────────────────
    lat = float(data.get("latitude", 0))
    lon = float(data.get("longitude", 0))
    if not (-90 <= lat <= 90):
        logger.warning(f"[Transform][ISS] Latitude inválida: {lat}. Usando 0.")
        lat = 0.0
    if not (-180 <= lon <= 180):
        logger.warning(f"[Transform][ISS] Longitude inválida: {lon}. Usando 0.")
        lon = 0.0

    # ── Conversão de timestamp Unix ───────────────────────────────
    ts_unix   = data.get("timestamp_unix", 0)
    ts_dt     = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat() \
                if ts_unix else datetime.now(timezone.utc).isoformat()

    # ── Monta registro normalizado ────────────────────────────────
    record = {
        "SATELLITE_NAME"  : str(data.get("satellite_name", "ISS"))[:50].upper(),
        "NORAD_ID"        : int(data.get("norad_id", 25544)),
        "LATITUDE"        : round(lat, 6),
        "LONGITUDE"       : round(lon, 6),
        "ALTITUDE_KM"     : float(data.get("altitude_km", 408.0)),
        "VELOCITY_KM_S"   : float(data.get("velocity_km_s", 7.66)),
        "ORBIT_TYPE"      : str(data.get("orbit_type", "LEO"))[:10],
        "STATUS"          : str(data.get("status", "UNKNOWN"))[:20].upper(),
        "CREW_COUNT"      : int(data.get("crew_count", 0)),
        "CREW_MEMBERS"    : ", ".join(data.get("crew_members", []))[:500],
        "OBS_TIMESTAMP"   : ts_dt,
        "EXTRACTED_AT"    : str(data.get("extracted_at", ""))[:50],
        "SOURCE"          : "OPEN_NOTIFY_API",
        "LOAD_DT"         : datetime.now(timezone.utc).isoformat(),
    }

    df = pd.DataFrame([record])
    out_path = os.path.join(out_dir, "iss_transformed.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"[Transform][ISS] 1 registro salvo em: {out_path}")
    return out_path


# ══════════════════════════════════════════════════════════════════
#  SpaceX
# ══════════════════════════════════════════════════════════════════

def _transform_spacex(raw_path: str, out_dir: str) -> str:
    """Normaliza e limpa registros de lançamentos SpaceX."""
    logger.info("[Transform] Processando dados SpaceX...")

    with open(raw_path, encoding="utf-8") as f:
        payload = json.load(f)

    launches = payload.get("launches", [])
    if not launches:
        logger.warning("[Transform][SpaceX] Nenhum lançamento encontrado no arquivo.")
        pd.DataFrame().to_csv(os.path.join(out_dir, "spacex_transformed.csv"), index=False)
        return os.path.join(out_dir, "spacex_transformed.csv")

    df = pd.DataFrame(launches)

    # ── Remover registros sem nome ou data ────────────────────────
    before = len(df)
    df = df[df["mission_name"].notna() & df["date_utc"].notna()]
    logger.info(f"[Transform][SpaceX] Removidos {before - len(df)} registros sem nome/data.")

    # ── Padronizar datas ──────────────────────────────────────────
    df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
    df = df[df["date_utc"].notna()]   # remove datas inválidas

    # ── Preencher nulos ───────────────────────────────────────────
    df["success"]        = df["success"].fillna(False).astype(bool)
    df["failure_reason"] = df["failure_reason"].fillna("N/A").str[:200]
    df["details"]        = df["details"].fillna("").str[:500]
    df["reused_booster"] = df["reused_booster"].fillna(False).astype(bool)
    df["payload_count"]  = df["payload_count"].fillna(0).astype(int)

    # ── Renomear colunas para Oracle (maiúsculas) ─────────────────
    df = df.rename(columns={
        "launch_id"      : "LAUNCH_ID",
        "flight_number"  : "FLIGHT_NUMBER",
        "mission_name"   : "MISSION_NAME",
        "date_utc"       : "LAUNCH_DATE_UTC",
        "rocket_id"      : "ROCKET_ID",
        "launchpad_id"   : "LAUNCHPAD_ID",
        "success"        : "IS_SUCCESS",
        "failure_reason" : "FAILURE_REASON",
        "details"        : "MISSION_DETAILS",
        "upcoming"       : "IS_UPCOMING",
        "reused_booster" : "IS_REUSED_BOOSTER",
        "landing_attempt": "LANDING_ATTEMPT",
        "landing_success": "LANDING_SUCCESS",
        "payload_count"  : "PAYLOAD_COUNT",
        "source"         : "SOURCE",
    })
    df["LOAD_DT"] = datetime.now(timezone.utc).isoformat()

    # Seleciona apenas colunas mapeadas
    keep_cols = ["LAUNCH_ID", "FLIGHT_NUMBER", "MISSION_NAME", "LAUNCH_DATE_UTC",
                 "ROCKET_ID", "LAUNCHPAD_ID", "IS_SUCCESS", "FAILURE_REASON",
                 "MISSION_DETAILS", "IS_UPCOMING", "IS_REUSED_BOOSTER",
                 "LANDING_ATTEMPT", "LANDING_SUCCESS", "PAYLOAD_COUNT",
                 "SOURCE", "LOAD_DT"]
    df = df[[c for c in keep_cols if c in df.columns]]

    out_path = os.path.join(out_dir, "spacex_transformed.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"[Transform][SpaceX] {len(df)} registros salvos em: {out_path}")
    return out_path


# ══════════════════════════════════════════════════════════════════
#  Telemetria Sintética
# ══════════════════════════════════════════════════════════════════

# Limites físicos razoáveis para satélites LEO
PHYSICAL_LIMITS = {
    "altitude_km"       : (300, 2000),
    "velocity_km_s"     : (6.5, 8.5),
    "panel_temp_c"      : (-60, 150),
    "battery_voltage_v" : (18, 36),
    "battery_current_a" : (0, 10),
    "signal_latency_ms" : (0, 2000),
}


def _flag_outlier(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona coluna IS_OUTLIER se qualquer campo exceder limites físicos."""
    df["IS_OUTLIER"] = False
    for col, (lo, hi) in PHYSICAL_LIMITS.items():
        if col in df.columns:
            df["IS_OUTLIER"] |= (df[col] < lo) | (df[col] > hi)
    return df


def _criticality(row: pd.Series) -> str:
    """Classifica criticidade com base em anomalia e status dos subsistemas."""
    if row.get("IS_ANOMALY", False):
        failed = sum(1 for s in ["SUBSYSTEM_ADCS", "SUBSYSTEM_EPS",
                                  "SUBSYSTEM_COM",  "SUBSYSTEM_OBC"]
                     if row.get(s) == "FAIL")
        if failed >= 2:
            return "CRITICAL"
        return "HIGH"
    warn = sum(1 for s in ["SUBSYSTEM_ADCS", "SUBSYSTEM_EPS",
                            "SUBSYSTEM_COM",  "SUBSYSTEM_OBC"]
               if row.get(s) == "WARN")
    return "MEDIUM" if warn >= 1 else "LOW"


def _transform_telemetry(raw_path: str, out_dir: str) -> str:
    """Limpa outliers, padroniza status e enriquece dados de telemetria."""
    logger.info("[Transform] Processando dados de telemetria...")

    with open(raw_path, encoding="utf-8") as f:
        payload = json.load(f)

    records = payload.get("telemetry", [])
    df = pd.DataFrame(records)

    if df.empty:
        logger.warning("[Transform][Telemetry] Dataset vazio.")
        pd.DataFrame().to_csv(os.path.join(out_dir, "telemetry_transformed.csv"), index=False)
        return os.path.join(out_dir, "telemetry_transformed.csv")

    # ── Tratar valores nulos ──────────────────────────────────────
    numeric_cols = ["latitude", "longitude", "altitude_km", "velocity_km_s",
                    "panel_temp_c", "battery_voltage_v", "battery_current_a",
                    "signal_latency_ms", "solar_exposure_pct"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(df[col].median())

    # ── Padronizar subsistemas (tudo maiúsculo) ───────────────────
    sub_cols = [c for c in df.columns if c.startswith("subsystem_")]
    for col in sub_cols:
        df[col] = df[col].fillna("UNKNOWN").str.upper().str[:10]

    # ── Converter timestamp ───────────────────────────────────────
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")

    # ── Detectar e marcar outliers físicos ───────────────────────
    df = _flag_outlier(df)
    outlier_count = df["IS_OUTLIER"].sum()
    logger.info(f"[Transform][Telemetry] Outliers detectados: {outlier_count}")

    # ── Enriquecimento: criticidade ───────────────────────────────
    df = df.rename(columns=str.upper)   # maiúsculas antes do apply
    df["CRITICALITY"] = df.apply(_criticality, axis=1)

    # ── Renomear coluna de anomalia para Oracle ───────────────────
    df["ANOMALY_TYPE"] = df.get("ANOMALY_TYPE", pd.Series([""] * len(df))).fillna("NONE")
    df["IS_ANOMALY"]   = df.get("IS_ANOMALY", pd.Series([False] * len(df))).astype(bool)
    df["LOAD_DT"]      = datetime.now(timezone.utc).isoformat()

    out_path = os.path.join(out_dir, "telemetry_transformed.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"[Transform][Telemetry] {len(df)} registros salvos em: {out_path}")
    return out_path


# ══════════════════════════════════════════════════════════════════
#  Entry Point
# ══════════════════════════════════════════════════════════════════

def transform_all(
    iss_path       : str,
    spacex_path    : str,
    telemetry_path : str,
    output_dir     : str,
) -> dict[str, str]:
    """
    Executa transformação de todas as fontes.

    Retorna
    -------
    dict
        Mapeamento {fonte: caminho_csv}.
    """
    os.makedirs(output_dir, exist_ok=True)

    results = {}
    results["iss"]       = _transform_iss(iss_path, output_dir)
    results["spacex"]    = _transform_spacex(spacex_path, output_dir)
    results["telemetry"] = _transform_telemetry(telemetry_path, output_dir)

    logger.info(f"[Transform] Transformação completa. Arquivos: {list(results.values())}")
    return results
