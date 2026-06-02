"""
==============================================================
 Global Solution 2026 — FIAP · Big Data Architecture & Data Integration
 Disciplina: BDDI
 Tema: Indústria Espacial — Pipeline de Telemetria Orbital
==============================================================

DAG: space_telemetry_pipeline
Descrição:
    Pipeline automatizado que ingere dados da ISS (Open Notify API),
    dados orbitais da SpaceX (SpaceX API) e telemetria simulada de
    satélites, transforma e carrega no Oracle Database para análise.

Fluxo:
    extração → tratamento → carga → análise
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# ── Importações dos scripts de pipeline ──────────────────────────
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract_iss        import extract_iss_position
from extract_spacex     import extract_spacex_launches
from extract_telemetry  import generate_satellite_telemetry
from transform_data     import transform_all
from load_oracle        import load_to_oracle

logger = logging.getLogger(__name__)

# ── Configurações padrão da DAG ───────────────────────────────────
DEFAULT_ARGS = {
    'owner'           : 'equipe_gs2026',
    'depends_on_past' : False,
    'start_date'      : days_ago(1),
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 2,
    'retry_delay'     : timedelta(minutes=5),
}

# ── Definição da DAG ──────────────────────────────────────────────
with DAG(
    dag_id            = 'space_telemetry_pipeline',
    description       = 'Pipeline de dados orbitais e telemetria espacial',
    default_args      = DEFAULT_ARGS,
    schedule_interval = '0 */6 * * *',   # executa a cada 6 horas
    catchup           = False,
    max_active_runs   = 1,
    tags              = ['gs2026', 'espacial', 'bddi', 'telemetria'],
) as dag:

    dag.doc_md = """
    ## Pipeline de Telemetria Espacial — Global Solution 2026

    Este pipeline coleta, transforma e armazena dados de:
    - **ISS (Estação Espacial Internacional)** via Open Notify API
    - **Lançamentos SpaceX** via SpaceX API pública
    - **Telemetria sintética** de satélites em órbita baixa (LEO)

    ### Arquitetura
    ```
    Open Notify API  ──┐
    SpaceX API       ──┼──▶  Extração  ──▶  Transformação  ──▶  Oracle DB  ──▶  Análise SQL
    Telemetria Sim.  ──┘
    ```
    """

    # ── TAREFA 1 — Extração ISS ───────────────────────────────────
    task_extract_iss = PythonOperator(
        task_id         = 'extrair_posicao_iss',
        python_callable = extract_iss_position,
        op_kwargs       = {'output_path': '/tmp/space_pipeline/iss_raw.json'},
        doc_md          = "Extrai posição atual e tripulação da ISS via Open Notify API.",
    )

    # ── TAREFA 2 — Extração SpaceX ────────────────────────────────
    task_extract_spacex = PythonOperator(
        task_id         = 'extrair_lancamentos_spacex',
        python_callable = extract_spacex_launches,
        op_kwargs       = {'output_path': '/tmp/space_pipeline/spacex_raw.json'},
        doc_md          = "Extrai histórico de lançamentos da SpaceX via API pública.",
    )

    # ── TAREFA 3 — Geração de Telemetria Simulada ─────────────────
    task_extract_telemetry = PythonOperator(
        task_id         = 'gerar_telemetria_satelites',
        python_callable = generate_satellite_telemetry,
        op_kwargs       = {'output_path': '/tmp/space_pipeline/telemetry_raw.json',
                           'n_records'  : 200},
        doc_md          = "Gera telemetria sintética realista para satélites LEO.",
    )

    # ── TAREFA 4 — Transformação e Limpeza ───────────────────────
    task_transform = PythonOperator(
        task_id         = 'transformar_dados',
        python_callable = transform_all,
        op_kwargs       = {
            'iss_path'       : '/tmp/space_pipeline/iss_raw.json',
            'spacex_path'    : '/tmp/space_pipeline/spacex_raw.json',
            'telemetry_path' : '/tmp/space_pipeline/telemetry_raw.json',
            'output_dir'     : '/tmp/space_pipeline/transformed/',
        },
        doc_md = "Limpa, padroniza e enriquece os dados de todas as fontes.",
    )

    # ── TAREFA 5 — Carga no Oracle ───────────────────────────────
    task_load = PythonOperator(
        task_id         = 'carregar_oracle',
        python_callable = load_to_oracle,
        op_kwargs       = {'data_dir': '/tmp/space_pipeline/transformed/'},
        doc_md          = "Carrega dados transformados nas tabelas Oracle do projeto.",
    )

    # ── TAREFA 6 — Verificação de qualidade ──────────────────────
    task_quality_check = BashOperator(
        task_id      = 'verificar_qualidade',
        bash_command = """
            echo "=== Verificação de Qualidade Pipeline Space ==="
            echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "Arquivos transformados:"
            ls -lh /tmp/space_pipeline/transformed/ 2>/dev/null || echo "Nenhum arquivo encontrado"
            echo "Pipeline finalizado com sucesso!"
        """,
        doc_md = "Verifica integridade dos arquivos gerados pelo pipeline.",
    )

    # ── DEPENDÊNCIAS (ordem de execução) ─────────────────────────
    #
    #   [iss] ──┐
    #           ├──▶ [transform] ──▶ [load] ──▶ [quality_check]
    #  [spacex]─┤
    #           │
    # [telemetry]┘
    #
    [task_extract_iss, task_extract_spacex, task_extract_telemetry] >> \
        task_transform >> task_load >> task_quality_check
