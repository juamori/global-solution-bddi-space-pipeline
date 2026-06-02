"""
extract_iss.py
==============
Extrai dados da Estação Espacial Internacional (ISS) via Open Notify API.

Fontes:
  - http://api.open-notify.org/iss-now.json  → posição atual (lat/lon/timestamp)
  - http://api.open-notify.org/astros.json   → tripulação a bordo

Saída:
  JSON com campos unificados gravado em output_path.
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
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────
ISS_POSITION_URL = "http://api.open-notify.org/iss-now.json"
ISS_CREW_URL     = "http://api.open-notify.org/astros.json"
REQUEST_TIMEOUT  = 15   # segundos
MAX_RETRIES      = 3


def _get_with_retry(url: str, retries: int = MAX_RETRIES) -> dict:
    """Faz requisição HTTP com retentativas em caso de falha."""
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"[ISS] Requisição para {url} (tentativa {attempt}/{retries})")
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning(f"[ISS] Tentativa {attempt} falhou: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)   # back-off exponencial
            else:
                raise


def extract_iss_position(output_path: str = "/tmp/space_pipeline/iss_raw.json") -> str:
    """
    Coleta posição da ISS e lista de astronautas a bordo.

    Parâmetros
    ----------
    output_path : str
        Caminho para salvar o JSON bruto.

    Retorna
    -------
    str
        Caminho do arquivo gravado.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    logger.info("[ISS] Iniciando extração de dados da ISS...")

    # 1. Posição orbital
    position_data = _get_with_retry(ISS_POSITION_URL)
    logger.info(f"[ISS] Posição recebida: lat={position_data['iss_position']['latitude']}, "
                f"lon={position_data['iss_position']['longitude']}")

    # 2. Tripulação
    crew_data = _get_with_retry(ISS_CREW_URL)
    iss_crew = [p for p in crew_data.get("people", []) if p.get("craft") == "ISS"]
    logger.info(f"[ISS] Tripulação a bordo: {len(iss_crew)} pessoas")

    # 3. Monta registro unificado
    record = {
        "source"          : "open_notify_api",
        "extracted_at"    : datetime.now(timezone.utc).isoformat(),
        "satellite_name"  : "ISS",
        "norad_id"        : 25544,
        "latitude"        : float(position_data["iss_position"]["latitude"]),
        "longitude"       : float(position_data["iss_position"]["longitude"]),
        "timestamp_unix"  : int(position_data["timestamp"]),
        "altitude_km"     : 408.0,          # altitude média ISS em km
        "velocity_km_s"   : 7.66,           # velocidade orbital média ISS
        "crew_count"      : len(iss_crew),
        "crew_members"    : [p["name"] for p in iss_crew],
        "orbit_type"      : "LEO",
        "status"          : "operational",
    }

    # 4. Grava JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"[ISS] Dados salvos em: {output_path}")
    return output_path
