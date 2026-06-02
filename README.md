# 🚀 Space Telemetry Pipeline — Global Solution 2026

**FIAP · Engenharia de Software · 4º Ano · Big Data Architecture & Data Integration**

> Pipeline automatizado de dados orbitais e telemetria espacial conectado à **Indústria Espacial** —
> usando Apache Airflow, Oracle Database e APIs abertas da NASA / SpaceX.

---

## 👥 Integrantes do Grupo

| Nome Completo | RM | Turma |
|---|---|---|
| João Rodrigo Solano Nogueira | RM 551319 | 4ESPV |
| Julia Amorim Bezerra | RM 99609 | 4ESPV |
| Lana Giulia Auada Leite | RM 551143 | 4ESPV |
| Tony Willian da Silva Segalin | RM 550667 | 4ESPV |

---

## 🎯 Contexto do Problema

A nova corrida espacial gera volumes massivos de dados orbitais diariamente. Satélites em órbita baixa
(LEO) transmitem telemetria continuamente — temperatura, tensão de bateria, posição, velocidade, status
de subsistemas. Esses dados precisam ser coletados, tratados e disponibilizados para análise em tempo
hábil.

**Problema:** Não existe um pipeline consolidado que ingira dados de múltiplas fontes espaciais (ISS,
lançamentos comerciais, telemetria de satélites), trate-os e os disponibilize para análise analítica.

**Solução:** Um pipeline ETL automatizado orquestrado por Apache Airflow, que coleta dados de APIs
públicas e telemetria simulada, os transforma e os carrega no Oracle Database para consultas analíticas.

**ODS conectados:**
- 🏗️ **ODS 9** — Inovação e infraestrutura (infraestrutura de dados orbitais)
- 🌍 **ODS 13** — Ação climática (monitoramento ambiental via satélite)

---

## 🏗️ Arquitetura do Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│                    FONTES DE DADOS                               │
│                                                                  │
│  Open Notify API    SpaceX API v4    Telemetria Sintética LEO    │
│  (ISS position)     (launches)       (CubeSats simulados)        │
└──────────┬──────────────┬───────────────────┬────────────────────┘
           │              │                   │
           ▼              ▼                   ▼
┌──────────────────────────────────────────────────────────────────┐
│              EXTRAÇÃO (Apache Airflow Tasks)                      │
│                                                                  │
│  extract_iss_position  extract_spacex_launches  gen_telemetry    │
│  [iss_raw.json]        [spacex_raw.json]         [telemetry.json] │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              TRANSFORMAÇÃO (transform_data.py)                   │
│                                                                  │
│  • Validação de coordenadas    • Detecção de outliers físicos    │
│  • Conversão de timestamps     • Classificação de criticidade    │
│  • Padronização de formatos    • Enriquecimento de metadados     │
│  • Tratamento de nulos         • Normalização de colunas         │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              CARGA (Oracle Database FIAP)                        │
│                                                                  │
│  TB_ISS_POSITION        TB_SPACEX_LAUNCHES    TB_SAT_TELEMETRY   │
│  (posição ISS)          (lançamentos)         (telemetria LEO)   │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              ANÁLISE (7 Queries SQL Analíticas)                  │
│                                                                  │
│  • Registros por período      • Estatísticas por satélite        │
│  • Ranking de anomalias       • Tendência de lançamentos SpaceX  │
│  • Cobertura orbital ISS      • Exposição solar vs falhas        │
│  • Dashboard executivo (JOIN)                                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📁 Estrutura do Repositório

```
space_pipeline/
├── dags/
│   └── space_telemetry_dag.py      # DAG principal do Airflow
├── scripts/
│   ├── extract_iss.py              # Extração Open Notify API (ISS)
│   ├── extract_spacex.py           # Extração SpaceX API v4
│   ├── extract_telemetry.py        # Gerador de telemetria sintética
│   ├── transform_data.py           # Transformação e limpeza
│   └── load_oracle.py              # Carga no Oracle Database
├── sql/
│   ├── 01_create_tables.sql        # DDL das tabelas Oracle
│   └── 02_analytical_queries.sql   # 7 consultas analíticas
├── data/
│   ├── telemetry_sample.csv        # Amostra de telemetria gerada
│   └── spacex_sample.csv           # Amostra de dados SpaceX
├── docs/
│   └── relatorio_bddi.pdf          # Relatório técnico completo
├── requirements.txt
└── README.md
```

---

## ⚙️ Como Executar

### Pré-requisitos
- Python 3.10+
- Apache Airflow 2.8+
- Oracle Database (FIAP): `oracle.fiap.com.br:1521/ORCL`
- Driver Oracle (`oracledb` ou `cx_Oracle`)

### 1. Instalar dependências
```bash
pip install -r requirements.txt
```

### 2. Configurar variáveis de ambiente
```bash
export ORACLE_USER=seu_rm
export ORACLE_PASSWORD=sua_senha
export ORACLE_HOST=oracle.fiap.com.br
export ORACLE_PORT=1521
export ORACLE_SID=ORCL
```

### 3. Criar tabelas no Oracle
```sql
-- Execute no SQL*Plus ou SQL Developer conectado ao FIAP Oracle:
@sql/01_create_tables.sql
```

### 4. Copiar DAG para o Airflow
```bash
cp dags/space_telemetry_dag.py $AIRFLOW_HOME/dags/
```

### 5. Inicializar o Airflow e ativar a DAG
```bash
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
# Acesse http://localhost:8080 e ative a DAG "space_telemetry_pipeline"
```

### 6. Executar consultas analíticas
```sql
@sql/02_analytical_queries.sql
```

---

## 🔌 Fontes de Dados

| Fonte | URL | Tipo | Frequência |
|---|---|---|---|---|
| Open Notify API | `http://api.open-notify.org/iss-now.json` | REST/JSON | Tempo real |
| Open Notify API | `http://api.open-notify.org/astros.json` | REST/JSON | Tempo real |
| SpaceX API v4 | `https://api.spacexdata.com/v4/launches` | REST/JSON | Diário |
| Telemetria LEO | Sintética (simulador interno) | Python | A cada execução |

---

## 🗄️ Modelagem das Tabelas Oracle

### TB_ISS_POSITION
Armazena snapshots de posição orbital e tripulação da ISS.

| Coluna | Tipo | Descrição |
|---|---|---|---|
| ID | NUMBER (PK) | Chave primária auto-gerada |
| SATELLITE_NAME | VARCHAR2(50) | Nome do satélite |
| NORAD_ID | NUMBER(10) | Identificador NORAD |
| LATITUDE | NUMBER(9,6) | Latitude geodésica (graus) |
| LONGITUDE | NUMBER(9,6) | Longitude geodésica (graus) |
| ALTITUDE_KM | NUMBER(8,3) | Altitude orbital em km |
| VELOCITY_KM_S | NUMBER(6,3) | Velocidade em km/s |
| CREW_COUNT | NUMBER(3) | Tripulantes a bordo |
| OBS_TIMESTAMP | TIMESTAMP WITH TZ | Momento da observação |

### TB_SPACEX_LAUNCHES
Histórico de lançamentos SpaceX com resultado e metadados.

| Coluna | Tipo | Descrição |
|---|---|---|---|
| LAUNCH_ID | VARCHAR2(50) | ID único do lançamento |
| MISSION_NAME | VARCHAR2(200) | Nome da missão |
| LAUNCH_DATE_UTC | TIMESTAMP WITH TZ | Data/hora de lançamento |
| IS_SUCCESS | CHAR(1) | Y/N/? |
| IS_REUSED_BOOSTER | CHAR(1) | Booster reutilizado? |
| PAYLOAD_COUNT | NUMBER(4) | Nº de payloads |

### TB_SAT_TELEMETRY
Telemetria completa dos satélites LEO simulados.

| Coluna | Tipo | Descrição |
|---|---|---|---|
| RECORD_ID | VARCHAR2(36) | UUID único da leitura |
| SATELLITE_NAME | VARCHAR2(50) | Nome do satélite |
| PANEL_TEMP_C | NUMBER(6,2) | Temperatura painel solar (°C) |
| BATTERY_VOLTAGE_V | NUMBER(6,3) | Tensão da bateria (V) |
| IS_ANOMALY | CHAR(1) | Anomalia detectada? |
| CRITICALITY | VARCHAR2(10) | LOW/MEDIUM/HIGH/CRITICAL |

---

## 📊 Consultas Analíticas (resumo)

| # | Título | Conceitos SQL |
|---|---|---|---|
| Q1 | Leituras por hora e satélite (últimas 24h) | GROUP BY, TRUNC, CASE WHEN |
| Q2 | Estatísticas físicas por satélite | AVG, MIN, MAX, STDDEV |
| Q3 | Ranking de anomalias por tipo | Window Function (OVER PARTITION BY) |
| Q4 | Taxa de sucesso SpaceX por ano | EXTRACT, HAVING, múltiplos CASE |
| Q5 | Cobertura orbital ISS por quadrante | CASE WHEN geográfico, múltiplos agregados |
| Q6 | Anomalias vs exposição solar | CASE WHEN, múltiplos SUM condicional |
| Q7 | Dashboard executivo integrado (JOIN) | CTE (WITH), CROSS JOIN, NULLIF |

---

## 🔁 Estrutura da DAG (Apache Airflow)

```
extrai_posicao_iss ────┐
                       │
extrai_lancamentos ────┼──▶  transformar_dados ──▶  carregar_oracle ──▶  verificar_qualidade
  _spacex               │
                       │
gera_telemetria  ──────┘
```

**Schedule:** `0 */6 * * *` — execução automática a cada 6 horas.

**Retentativas:** 2 tentativas com intervalo de 5 minutos em caso de falha.

---

## 📦 Dependências (requirements.txt)

```
apache-airflow==2.8.1
pandas==2.1.4
requests==2.31.0
oracledb==1.4.2
```
