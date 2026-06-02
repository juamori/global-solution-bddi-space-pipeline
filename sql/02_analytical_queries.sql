-- ================================================================
--  Global Solution 2026 — FIAP · BDDI
--  Script: 02_analytical_queries.sql
--  Descrição: Consultas analíticas sobre os dados orbitais/espaciais
--             Oracle Database — oracle.fiap.com.br:1521/ORCL
-- ================================================================

-- ════════════════════════════════════════════════════════════════
-- QUERY 1 — Quantidade de registros processados por período
--           (agrupamento diário de leituras de telemetria)
-- ════════════════════════════════════════════════════════════════
SELECT
    TRUNC(TIMESTAMP_UTC, 'HH24')                    AS HORA_UTC,
    SATELLITE_NAME,
    COUNT(*)                                         AS TOTAL_LEITURAS,
    SUM(CASE WHEN IS_ANOMALY = 'Y' THEN 1 ELSE 0 END) AS TOTAL_ANOMALIAS,
    ROUND(
        SUM(CASE WHEN IS_ANOMALY = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                                AS PCT_ANOMALIAS
FROM
    TB_SAT_TELEMETRY
WHERE
    TIMESTAMP_UTC >= SYSTIMESTAMP - INTERVAL '24' HOUR
GROUP BY
    TRUNC(TIMESTAMP_UTC, 'HH24'),
    SATELLITE_NAME
ORDER BY
    HORA_UTC DESC,
    SATELLITE_NAME;


-- ════════════════════════════════════════════════════════════════
-- QUERY 2 — Estatísticas físicas por satélite
--           (média, mínimo, máximo de variáveis-chave)
-- ════════════════════════════════════════════════════════════════
SELECT
    SATELLITE_NAME,
    ORBIT_TYPE,
    COUNT(*)                                AS TOTAL_REGISTROS,
    ROUND(AVG(ALTITUDE_KM),       2)        AS ALTITUDE_MEDIA_KM,
    ROUND(MIN(ALTITUDE_KM),       2)        AS ALTITUDE_MIN_KM,
    ROUND(MAX(ALTITUDE_KM),       2)        AS ALTITUDE_MAX_KM,
    ROUND(AVG(PANEL_TEMP_C),      2)        AS TEMP_PAINEL_MEDIA_C,
    ROUND(MIN(PANEL_TEMP_C),      2)        AS TEMP_PAINEL_MIN_C,
    ROUND(MAX(PANEL_TEMP_C),      2)        AS TEMP_PAINEL_MAX_C,
    ROUND(AVG(BATTERY_VOLTAGE_V), 3)        AS TENSAO_MEDIA_V,
    ROUND(MIN(BATTERY_VOLTAGE_V), 3)        AS TENSAO_MIN_V,
    ROUND(MAX(BATTERY_VOLTAGE_V), 3)        AS TENSAO_MAX_V,
    ROUND(AVG(SIGNAL_LATENCY_MS), 1)        AS LATENCIA_MEDIA_MS,
    ROUND(STDDEV(SIGNAL_LATENCY_MS), 2)     AS LATENCIA_DESVPAD_MS
FROM
    TB_SAT_TELEMETRY
GROUP BY
    SATELLITE_NAME,
    ORBIT_TYPE
ORDER BY
    TOTAL_REGISTROS DESC;


-- ════════════════════════════════════════════════════════════════
-- QUERY 3 — Ranking de anomalias por satélite e tipo
--           (identifica satélites mais problemáticos)
-- ════════════════════════════════════════════════════════════════
SELECT
    SATELLITE_NAME,
    ANOMALY_TYPE,
    CRITICALITY,
    COUNT(*)                AS OCORRENCIAS,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY SATELLITE_NAME),
        2
    )                       AS PCT_DO_SATELITE,
    MIN(TIMESTAMP_UTC)      AS PRIMEIRA_OCORRENCIA,
    MAX(TIMESTAMP_UTC)      AS ULTIMA_OCORRENCIA,
    ROUND(
        (MAX(TIMESTAMP_UTC) - MIN(TIMESTAMP_UTC)) * 24 * 60,
        1
    )                       AS DURACAO_MONITORAMENTO_MIN
FROM
    TB_SAT_TELEMETRY
WHERE
    IS_ANOMALY = 'Y'
GROUP BY
    SATELLITE_NAME,
    ANOMALY_TYPE,
    CRITICALITY
ORDER BY
    OCORRENCIAS DESC,
    CRITICALITY,
    SATELLITE_NAME;


-- ════════════════════════════════════════════════════════════════
-- QUERY 4 — Análise temporal de lançamentos SpaceX
--           (taxa de sucesso por ano e tipo de foguete)
-- ════════════════════════════════════════════════════════════════
SELECT
    EXTRACT(YEAR FROM LAUNCH_DATE_UTC)                       AS ANO_LANCAMENTO,
    ROCKET_ID,
    COUNT(*)                                                 AS TOTAL_LANCAMENTOS,
    SUM(CASE WHEN IS_SUCCESS = 'Y' THEN 1 ELSE 0 END)       AS SUCESSOS,
    SUM(CASE WHEN IS_SUCCESS = 'N' THEN 1 ELSE 0 END)       AS FALHAS,
    ROUND(
        SUM(CASE WHEN IS_SUCCESS = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                                        AS TAXA_SUCESSO_PCT,
    SUM(CASE WHEN IS_REUSED_BOOSTER = 'Y' THEN 1 ELSE 0 END) AS BOOSTERS_REUSADOS,
    SUM(CASE WHEN LANDING_SUCCESS  = 'Y' THEN 1 ELSE 0 END)  AS POUSAGENS_SUCESSO,
    SUM(PAYLOAD_COUNT)                                       AS TOTAL_PAYLOADS
FROM
    TB_SPACEX_LAUNCHES
WHERE
    IS_UPCOMING = 'N'
    AND LAUNCH_DATE_UTC IS NOT NULL
GROUP BY
    EXTRACT(YEAR FROM LAUNCH_DATE_UTC),
    ROCKET_ID
HAVING
    COUNT(*) >= 1
ORDER BY
    ANO_LANCAMENTO DESC,
    TOTAL_LANCAMENTOS DESC;


-- ════════════════════════════════════════════════════════════════
-- QUERY 5 — Padrão geográfico de cobertura orbital da ISS
--           (distribuição de posições por quadrante do globo)
-- ════════════════════════════════════════════════════════════════
SELECT
    CASE
        WHEN LATITUDE  >= 0 AND LONGITUDE >= 0 THEN 'NORDESTE (+lat, +lon)'
        WHEN LATITUDE  >= 0 AND LONGITUDE <  0 THEN 'NOROESTE (+lat, -lon)'
        WHEN LATITUDE  <  0 AND LONGITUDE >= 0 THEN 'SUDESTE  (-lat, +lon)'
        ELSE                                        'SUDOESTE  (-lat, -lon)'
    END                                 AS QUADRANTE_ORBITAL,
    COUNT(*)                            AS REGISTROS,
    ROUND(AVG(LATITUDE),  4)            AS LAT_MEDIA,
    ROUND(AVG(LONGITUDE), 4)            AS LON_MEDIA,
    ROUND(MIN(LATITUDE),  4)            AS LAT_MIN,
    ROUND(MAX(LATITUDE),  4)            AS LAT_MAX,
    ROUND(MIN(LONGITUDE), 4)            AS LON_MIN,
    ROUND(MAX(LONGITUDE), 4)            AS LON_MAX,
    ROUND(AVG(ALTITUDE_KM), 2)          AS ALTITUDE_MEDIA_KM,
    MIN(OBS_TIMESTAMP)                  AS PRIMEIRA_OBS,
    MAX(OBS_TIMESTAMP)                  AS ULTIMA_OBS
FROM
    TB_ISS_POSITION
GROUP BY
    CASE
        WHEN LATITUDE  >= 0 AND LONGITUDE >= 0 THEN 'NORDESTE (+lat, +lon)'
        WHEN LATITUDE  >= 0 AND LONGITUDE <  0 THEN 'NOROESTE (+lat, -lon)'
        WHEN LATITUDE  <  0 AND LONGITUDE >= 0 THEN 'SUDESTE  (-lat, +lon)'
        ELSE                                        'SUDOESTE  (-lat, -lon)'
    END
ORDER BY
    REGISTROS DESC;


-- ════════════════════════════════════════════════════════════════
-- QUERY 6 — Comparação de períodos: anomalias diurnas vs noturnas
--           (exposição solar x ocorrência de falhas)
-- ════════════════════════════════════════════════════════════════
SELECT
    CASE
        WHEN SOLAR_EXPOSURE_PCT >= 70 THEN 'PLENA EXPOSIÇÃO (≥70%)'
        WHEN SOLAR_EXPOSURE_PCT >= 30 THEN 'EXPOSIÇÃO PARCIAL (30-70%)'
        ELSE                               'ECLIPSE/SOMBRA (<30%)'
    END                                       AS CONDICAO_SOLAR,
    COUNT(*)                                  AS TOTAL_LEITURAS,
    SUM(CASE WHEN IS_ANOMALY = 'Y' THEN 1 ELSE 0 END) AS ANOMALIAS,
    ROUND(
        SUM(CASE WHEN IS_ANOMALY = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                         AS PCT_ANOMALIA,
    ROUND(AVG(PANEL_TEMP_C),      2)          AS TEMP_MEDIA_C,
    ROUND(AVG(BATTERY_VOLTAGE_V), 3)          AS TENSAO_MEDIA_V,
    ROUND(AVG(SIGNAL_LATENCY_MS), 1)          AS LATENCIA_MEDIA_MS,
    -- Subsistemas mais afetados
    SUM(CASE WHEN SUBSYSTEM_EPS     = 'FAIL' THEN 1 ELSE 0 END) AS FALHAS_EPS,
    SUM(CASE WHEN SUBSYSTEM_THERMAL = 'FAIL' THEN 1 ELSE 0 END) AS FALHAS_THERMAL,
    SUM(CASE WHEN SUBSYSTEM_COM     = 'FAIL' THEN 1 ELSE 0 END) AS FALHAS_COM
FROM
    TB_SAT_TELEMETRY
GROUP BY
    CASE
        WHEN SOLAR_EXPOSURE_PCT >= 70 THEN 'PLENA EXPOSIÇÃO (≥70%)'
        WHEN SOLAR_EXPOSURE_PCT >= 30 THEN 'EXPOSIÇÃO PARCIAL (30-70%)'
        ELSE                               'ECLIPSE/SOMBRA (<30%)'
    END
ORDER BY
    PCT_ANOMALIA DESC;


-- ════════════════════════════════════════════════════════════════
-- QUERY 7 — Dashboard executivo: visão consolidada do pipeline
--           (JOIN entre fontes para visão integrada)
-- ════════════════════════════════════════════════════════════════
WITH anomaly_summary AS (
    SELECT
        SATELLITE_NAME,
        COUNT(*)                                                     AS TOTAL_TEL,
        SUM(CASE WHEN IS_ANOMALY  = 'Y' THEN 1 ELSE 0 END)         AS TOTAL_ANOMALIAS,
        SUM(CASE WHEN CRITICALITY = 'CRITICAL' THEN 1 ELSE 0 END)  AS CRITICOS,
        SUM(CASE WHEN IS_OUTLIER  = 'Y' THEN 1 ELSE 0 END)         AS OUTLIERS,
        ROUND(AVG(ALTITUDE_KM),    2)                               AS ALT_MEDIA,
        ROUND(AVG(BATTERY_VOLTAGE_V), 3)                            AS TENSAO_MEDIA,
        MAX(TIMESTAMP_UTC)                                          AS ULTIMA_LEITURA
    FROM
        TB_SAT_TELEMETRY
    GROUP BY
        SATELLITE_NAME
),
spacex_summary AS (
    SELECT
        COUNT(*)                                                 AS TOTAL_LAUNCHES,
        SUM(CASE WHEN IS_SUCCESS = 'Y' THEN 1 ELSE 0 END)      AS SUCESSOS,
        SUM(PAYLOAD_COUNT)                                       AS TOTAL_PAYLOADS,
        MAX(LAUNCH_DATE_UTC)                                     AS ULTIMO_LANCAMENTO
    FROM
        TB_SPACEX_LAUNCHES
    WHERE IS_UPCOMING = 'N'
),
iss_summary AS (
    SELECT
        COUNT(*)                AS TOTAL_SNAPSHOTS,
        ROUND(AVG(LATITUDE), 4) AS LAT_MEDIA,
        MAX(CREW_COUNT)         AS MAX_TRIPULANTES,
        MAX(OBS_TIMESTAMP)      AS ULTIMO_SNAPSHOT
    FROM
        TB_ISS_POSITION
)
SELECT
    -- Satélites simulados
    a.SATELLITE_NAME,
    a.TOTAL_TEL               AS "LEITURAS TELEMETRIA",
    a.TOTAL_ANOMALIAS         AS "ANOMALIAS",
    a.CRITICOS                AS "CRÍTICOS",
    ROUND(a.TOTAL_ANOMALIAS * 100.0 / NULLIF(a.TOTAL_TEL, 0), 2) AS "% ANOMALIA",
    a.ALT_MEDIA               AS "ALT MÉDIA (km)",
    a.TENSAO_MEDIA            AS "TENSÃO MÉDIA (V)",
    a.ULTIMA_LEITURA          AS "ÚLTIMA LEITURA",
    -- Contexto SpaceX (repetido por linha — visão consolidada)
    s.TOTAL_LAUNCHES          AS "LANÇAMENTOS SPACEX",
    s.SUCESSOS                AS "SUCESSOS SPACEX",
    s.TOTAL_PAYLOADS          AS "PAYLOADS ENVIADOS",
    -- Contexto ISS
    i.TOTAL_SNAPSHOTS         AS "SNAPSHOTS ISS",
    i.MAX_TRIPULANTES         AS "TRIPULANTES ISS"
FROM
    anomaly_summary a
    CROSS JOIN spacex_summary s
    CROSS JOIN iss_summary    i
ORDER BY
    a.TOTAL_ANOMALIAS DESC;
