"""
extract_spacex.py
=================
Extrai dados de lançamentos da SpaceX via API pública (r-spacex).

Fonte:
  https://api.spacexdata.com/v4/launches

Saída:
  JSON com lista dos últimos N lançamentos gravado em output_path.
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
from typing import Optional

import requests

logger = logging.getLogger(__name__)

SPACEX_LAUNCHES_URL = "https://api.spacexdata.com/v4/launches"
REQUEST_TIMEOUT     = 20
MAX_RETRIES         = 3
DEFAULT_LIMIT       = 50   # quantos lançamentos buscar


def _get_with_retry(url: str, params: dict = None, retries: int = MAX_RETRIES) -> list | dict:
    """Requisição HTTP com retentativas e back-off exponencial."""
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"[SpaceX] Requisição para {url} (tentativa {attempt}/{retries})")
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning(f"[SpaceX] Tentativa {attempt} falhou: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                logger.error("[SpaceX] Todas as tentativas esgotadas. Retornando lista vazia.")
                return []


def _parse_launch(raw: dict) -> Optional[dict]:
    """
    Transforma um lançamento bruto da API em registro padronizado.
    Retorna None se os dados mínimos estiverem ausentes.
    """
    launch_id = raw.get("id") or raw.get("flight_number")
    if not launch_id:
        return None

    date_utc  = raw.get("date_utc", "")
    success   = raw.get("success")
    failures  = raw.get("failures", [])

    return {
        "launch_id"        : str(launch_id),
        "flight_number"    : raw.get("flight_number"),
        "mission_name"     : raw.get("name", "UNKNOWN"),
        "date_utc"         : date_utc,
        "date_local"       : raw.get("date_local", ""),
        "rocket_id"        : raw.get("rocket", ""),
        "launchpad_id"     : raw.get("launchpad", ""),
        "success"          : success,
        "failure_reason"   : failures[0].get("reason", "") if failures else "",
        "webcast_url"      : (raw.get("links") or {}).get("webcast", ""),
        "wikipedia_url"    : (raw.get("links") or {}).get("wikipedia", ""),
        "details"          : (raw.get("details") or "")[:500],   # limita a 500 chars
        "upcoming"         : raw.get("upcoming", False),
        "reused_booster"   : bool((raw.get("cores") or [{}])[0].get("reused", False)),
        "landing_attempt"  : bool((raw.get("cores") or [{}])[0].get("landing_attempt", False)),
        "landing_success"  : (raw.get("cores") or [{}])[0].get("landing_success"),
        "payload_count"    : len(raw.get("payloads") or []),
        "source"           : "spacex_api_v4",
        "extracted_at"     : datetime.now(timezone.utc).isoformat(),
    }


def extract_spacex_launches(
    output_path : str = "/tmp/space_pipeline/spacex_raw.json",
    limit       : int = DEFAULT_LIMIT,
) -> str:
    """
    Coleta lançamentos SpaceX e salva em JSON.

    Parâmetros
    ----------
    output_path : str
        Caminho para salvar o JSON bruto.
    limit : int
        Número máximo de lançamentos a coletar.

    Retorna
    -------
    str
        Caminho do arquivo gravado.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    logger.info(f"[SpaceX] Iniciando extração (limite: {limit} registros)...")

    raw_launches = _get_with_retry(SPACEX_LAUNCHES_URL)

    # Ordena por data decrescente e limita
    try:
        raw_launches = sorted(
            raw_launches,
            key=lambda x: x.get("date_utc", ""),
            reverse=True,
        )[:limit]
    except (TypeError, KeyError):
        pass

    parsed = []
    skipped = 0
    for raw in raw_launches:
        record = _parse_launch(raw)
        if record:
            parsed.append(record)
        else:
            skipped += 1

    logger.info(f"[SpaceX] Registros processados: {len(parsed)} | Ignorados: {skipped}")

    payload = {
        "source"       : "spacex_api_v4",
        "extracted_at" : datetime.now(timezone.utc).isoformat(),
        "total_records": len(parsed),
        "launches"     : parsed,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"[SpaceX] Dados salvos em: {output_path}")
    return output_path
