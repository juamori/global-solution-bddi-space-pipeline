"""
extract_telemetry.py
====================
Gera telemetria sintética realista de satélites em órbita baixa (LEO).

Simula sensores comuns de nanossatélites/CubeSats:
  - temperatura dos painéis solares
  - tensão / corrente da bateria
  - altitude orbital
  - velocidade
  - latência do sinal
  - status de subsistemas (ADCS, EPS, COM, OBC)
  - evento de anomalia (flag booleano)

Saída:
  JSON com lista de n_records registros, gravado em output_path.
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
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ── Frota de satélites simulados ──────────────────────────────────
SATELLITES = [
    {"name": "AgroSat-1",  "norad_id": 99001, "orbit_type": "LEO", "altitude_base": 520},
    {"name": "AgroSat-2",  "norad_id": 99002, "orbit_type": "LEO", "altitude_base": 525},
    {"name": "EcoWatch-A", "norad_id": 99003, "orbit_type": "LEO", "altitude_base": 600},
    {"name": "EcoWatch-B", "norad_id": 99004, "orbit_type": "LEO", "altitude_base": 598},
    {"name": "ConnectSat", "norad_id": 99005, "orbit_type": "LEO", "altitude_base": 550},
    {"name": "WeatherEye", "norad_id": 99006, "orbit_type": "SSO", "altitude_base": 700},
    {"name": "NanoDebris",  "norad_id": 99007, "orbit_type": "LEO", "altitude_base": 480},
]

SUBSYSTEM_NAMES = ["ADCS", "EPS", "COM", "OBC", "THERMAL", "PAYLOAD"]


def _simulate_orbit(sat: dict, t_offset_min: float) -> tuple[float, float]:
    """
    Simula posição orbital simplificada.
    Usa modelo circular com período de ~94 min (LEO típico).
    """
    period_min  = 2 * math.pi * math.sqrt((sat["altitude_base"] + 6371) ** 3 / 398600.4418) / 60
    angle_rad   = (2 * math.pi * t_offset_min) / period_min
    lat         = math.degrees(math.asin(math.sin(51.6 * math.pi / 180) * math.sin(angle_rad)))
    lon         = (math.degrees(angle_rad) % 360) - 180
    return round(lat, 6), round(lon, 6)


def _random_telemetry_record(sat: dict, timestamp: datetime, t_offset_min: float) -> dict:
    """Gera um registro de telemetria para um satélite em dado instante."""
    lat, lon = _simulate_orbit(sat, t_offset_min)

    # Anomalia esporádica (5 % de chance)
    is_anomaly     = random.random() < 0.05
    anomaly_type   = random.choice(["thermal_spike", "battery_drop", "signal_loss", "radiation_event"]) \
                     if is_anomaly else None

    # Temperatura varia com exposição solar (simplificado)
    solar_exposure = (math.sin(t_offset_min * 0.067) + 1) / 2   # 0 a 1
    panel_temp     = round(-40 + solar_exposure * 160 + random.gauss(0, 3), 2)   # -40 a 120 °C

    battery_voltage = round(random.gauss(28.5, 0.5), 3)   # volts
    battery_current = round(random.gauss(2.1, 0.3), 3)    # amperes
    if is_anomaly and anomaly_type == "battery_drop":
        battery_voltage = round(random.uniform(22, 25), 3)

    altitude   = round(sat["altitude_base"] + random.gauss(0, 1.5), 3)
    velocity   = round(7.784 * math.sqrt(6371 / (6371 + altitude)), 3)   # km/s
    latency_ms = round(random.gauss(240, 20) + (altitude / 500) * 15, 1)

    subsystems = {s: random.choice(["OK", "OK", "OK", "OK", "WARN", "FAIL"])
                  for s in SUBSYSTEM_NAMES}
    if is_anomaly:
        faulty = random.choice(SUBSYSTEM_NAMES)
        subsystems[faulty] = "FAIL"

    return {
        "record_id"         : str(uuid.uuid4()),
        "satellite_name"    : sat["name"],
        "norad_id"          : sat["norad_id"],
        "orbit_type"        : sat["orbit_type"],
        "timestamp_utc"     : timestamp.isoformat(),
        "latitude"          : lat,
        "longitude"         : lon,
        "altitude_km"       : altitude,
        "velocity_km_s"     : velocity,
        "panel_temp_c"      : panel_temp,
        "battery_voltage_v" : battery_voltage,
        "battery_current_a" : battery_current,
        "signal_latency_ms" : latency_ms,
        "solar_exposure_pct": round(solar_exposure * 100, 1),
        "subsystem_adcs"    : subsystems["ADCS"],
        "subsystem_eps"     : subsystems["EPS"],
        "subsystem_com"     : subsystems["COM"],
        "subsystem_obc"     : subsystems["OBC"],
        "subsystem_thermal" : subsystems["THERMAL"],
        "subsystem_payload" : subsystems["PAYLOAD"],
        "is_anomaly"        : is_anomaly,
        "anomaly_type"      : anomaly_type,
        "source"            : "synthetic_telemetry_simulator",
    }


def generate_satellite_telemetry(
    output_path : str = "/tmp/space_pipeline/telemetry_raw.json",
    n_records   : int = 200,
) -> str:
    """
    Gera telemetria sintética e salva em JSON.

    Parâmetros
    ----------
    output_path : str
        Caminho de saída.
    n_records : int
        Quantidade de registros a gerar (mínimo recomendado: 200).

    Retorna
    -------
    str
        Caminho do arquivo gravado.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    random.seed(42)   # reprodutibilidade

    logger.info(f"[Telemetry] Gerando {n_records} registros de telemetria sintética...")

    records = []
    base_ts = datetime.now(timezone.utc) - timedelta(hours=24)

    for i in range(n_records):
        sat           = SATELLITES[i % len(SATELLITES)]
        t_offset_min  = i * 2.8   # leituras a cada ~2,8 min
        timestamp     = base_ts + timedelta(minutes=t_offset_min)
        record        = _random_telemetry_record(sat, timestamp, t_offset_min)
        records.append(record)

    anomaly_count = sum(1 for r in records if r["is_anomaly"])
    logger.info(f"[Telemetry] Gerados: {n_records} registros | Anomalias: {anomaly_count}")

    payload = {
        "source"         : "synthetic_telemetry_simulator",
        "generated_at"   : datetime.now(timezone.utc).isoformat(),
        "total_records"  : len(records),
        "anomaly_count"  : anomaly_count,
        "satellites"     : [s["name"] for s in SATELLITES],
        "telemetry"      : records,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"[Telemetry] Arquivo salvo em: {output_path}")
    return output_path
