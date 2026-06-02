-- ================================================================
--  Global Solution 2026 — FIAP · BDDI
--  Disciplina: Big Data Architecture & Data Integration
--  Tema: Indústria Espacial — Pipeline de Telemetria Orbital
--
--  Script: 01_create_tables.sql
--  Descrição: DDL das tabelas do projeto no Oracle Database FIAP
--             Host: oracle.fiap.com.br | Porta: 1521 | SID: ORCL
-- ================================================================

-- ────────────────────────────────────────────────────────────────
-- 1. TB_ISS_POSITION
--    Armazena snapshots da posição orbital da ISS e tripulação.
-- ────────────────────────────────────────────────────────────────
CREATE TABLE TB_ISS_POSITION (
    ID               NUMBER         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SATELLITE_NAME   VARCHAR2(50)   NOT NULL,
    NORAD_ID         NUMBER(10)     NOT NULL,
    LATITUDE         NUMBER(9,6)    NOT NULL,
    LONGITUDE        NUMBER(9,6)    NOT NULL,
    ALTITUDE_KM      NUMBER(8,3),
    VELOCITY_KM_S    NUMBER(6,3),
    ORBIT_TYPE       VARCHAR2(10),
    STATUS           VARCHAR2(20),
    CREW_COUNT       NUMBER(3),
    CREW_MEMBERS     VARCHAR2(500),
    OBS_TIMESTAMP    TIMESTAMP WITH TIME ZONE,
    EXTRACTED_AT     VARCHAR2(50),
    SOURCE           VARCHAR2(30)   DEFAULT 'OPEN_NOTIFY_API',
    LOAD_DT          TIMESTAMP      DEFAULT SYSTIMESTAMP
);

COMMENT ON TABLE  TB_ISS_POSITION                IS 'Snapshots de posição orbital da ISS — Open Notify API';
COMMENT ON COLUMN TB_ISS_POSITION.LATITUDE       IS 'Latitude geodésica em graus decimais';
COMMENT ON COLUMN TB_ISS_POSITION.LONGITUDE      IS 'Longitude geodésica em graus decimais';
COMMENT ON COLUMN TB_ISS_POSITION.ALTITUDE_KM    IS 'Altitude orbital média em km';
COMMENT ON COLUMN TB_ISS_POSITION.VELOCITY_KM_S  IS 'Velocidade orbital em km/s';
COMMENT ON COLUMN TB_ISS_POSITION.CREW_COUNT     IS 'Número de astronautas a bordo no momento da extração';
COMMENT ON COLUMN TB_ISS_POSITION.CREW_MEMBERS   IS 'Nomes dos tripulantes separados por vírgula';

-- ────────────────────────────────────────────────────────────────
-- 2. TB_SPACEX_LAUNCHES
--    Armazena lançamentos históricos e futuros da SpaceX.
-- ────────────────────────────────────────────────────────────────
CREATE TABLE TB_SPACEX_LAUNCHES (
    ID               NUMBER         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    LAUNCH_ID        VARCHAR2(50)   NOT NULL,
    FLIGHT_NUMBER    NUMBER(6),
    MISSION_NAME     VARCHAR2(200)  NOT NULL,
    LAUNCH_DATE_UTC  TIMESTAMP WITH TIME ZONE,
    ROCKET_ID        VARCHAR2(50),
    LAUNCHPAD_ID     VARCHAR2(50),
    IS_SUCCESS       CHAR(1)        CHECK (IS_SUCCESS IN ('Y','N','?')),
    FAILURE_REASON   VARCHAR2(200),
    MISSION_DETAILS  VARCHAR2(500),
    IS_UPCOMING      CHAR(1)        CHECK (IS_UPCOMING IN ('Y','N')),
    IS_REUSED_BOOSTER CHAR(1)       CHECK (IS_REUSED_BOOSTER IN ('Y','N')),
    LANDING_ATTEMPT  CHAR(1)        CHECK (LANDING_ATTEMPT IN ('Y','N')),
    LANDING_SUCCESS  CHAR(1)        CHECK (LANDING_SUCCESS IN ('Y','N','?')),
    PAYLOAD_COUNT    NUMBER(4)      DEFAULT 0,
    SOURCE           VARCHAR2(30)   DEFAULT 'SPACEX_API_V4',
    LOAD_DT          TIMESTAMP      DEFAULT SYSTIMESTAMP,
    CONSTRAINT UQ_SPACEX_LAUNCH_ID UNIQUE (LAUNCH_ID)
);

COMMENT ON TABLE  TB_SPACEX_LAUNCHES               IS 'Lançamentos SpaceX — SpaceX Open API v4';
COMMENT ON COLUMN TB_SPACEX_LAUNCHES.IS_SUCCESS     IS 'Y=Sucesso, N=Falha, ?=Indefinido';
COMMENT ON COLUMN TB_SPACEX_LAUNCHES.IS_UPCOMING    IS 'Y=Futuro, N=Realizado';
COMMENT ON COLUMN TB_SPACEX_LAUNCHES.PAYLOAD_COUNT  IS 'Quantidade de payloads no lançamento';

-- ────────────────────────────────────────────────────────────────
-- 3. TB_SAT_TELEMETRY
--    Armazena telemetria dos satélites simulados (LEO/SSO).
-- ────────────────────────────────────────────────────────────────
CREATE TABLE TB_SAT_TELEMETRY (
    ID                  NUMBER        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    RECORD_ID           VARCHAR2(36)  NOT NULL,
    SATELLITE_NAME      VARCHAR2(50)  NOT NULL,
    NORAD_ID            NUMBER(10),
    ORBIT_TYPE          VARCHAR2(10),
    TIMESTAMP_UTC       TIMESTAMP WITH TIME ZONE,
    LATITUDE            NUMBER(9,6),
    LONGITUDE           NUMBER(9,6),
    ALTITUDE_KM         NUMBER(8,3),
    VELOCITY_KM_S       NUMBER(6,3),
    PANEL_TEMP_C        NUMBER(6,2),
    BATTERY_VOLTAGE_V   NUMBER(6,3),
    BATTERY_CURRENT_A   NUMBER(5,3),
    SIGNAL_LATENCY_MS   NUMBER(7,1),
    SOLAR_EXPOSURE_PCT  NUMBER(5,1),
    SUBSYSTEM_ADCS      VARCHAR2(10),
    SUBSYSTEM_EPS       VARCHAR2(10),
    SUBSYSTEM_COM       VARCHAR2(10),
    SUBSYSTEM_OBC       VARCHAR2(10),
    SUBSYSTEM_THERMAL   VARCHAR2(10),
    SUBSYSTEM_PAYLOAD   VARCHAR2(10),
    IS_ANOMALY          CHAR(1)       CHECK (IS_ANOMALY IN ('Y','N')),
    ANOMALY_TYPE        VARCHAR2(30),
    CRITICALITY         VARCHAR2(10)  CHECK (CRITICALITY IN ('LOW','MEDIUM','HIGH','CRITICAL')),
    IS_OUTLIER          CHAR(1)       CHECK (IS_OUTLIER IN ('Y','N')),
    SOURCE              VARCHAR2(40)  DEFAULT 'SYNTHETIC_TELEMETRY',
    LOAD_DT             TIMESTAMP     DEFAULT SYSTIMESTAMP,
    CONSTRAINT UQ_SAT_TELEMETRY_RECORD UNIQUE (RECORD_ID)
);

COMMENT ON TABLE  TB_SAT_TELEMETRY                IS 'Telemetria sintética de satélites LEO — GS2026';
COMMENT ON COLUMN TB_SAT_TELEMETRY.PANEL_TEMP_C   IS 'Temperatura dos painéis solares em graus Celsius';
COMMENT ON COLUMN TB_SAT_TELEMETRY.IS_ANOMALY      IS 'Y=Anomalia detectada, N=Normal';
COMMENT ON COLUMN TB_SAT_TELEMETRY.CRITICALITY     IS 'Classificação de criticidade: LOW/MEDIUM/HIGH/CRITICAL';
COMMENT ON COLUMN TB_SAT_TELEMETRY.IS_OUTLIER      IS 'Y=Valor fora dos limites físicos esperados';

-- ────────────────────────────────────────────────────────────────
-- Índices para performance das queries analíticas
-- ────────────────────────────────────────────────────────────────
CREATE INDEX IDX_ISS_OBS_TS      ON TB_ISS_POSITION   (OBS_TIMESTAMP);
CREATE INDEX IDX_ISS_LOAD_DT     ON TB_ISS_POSITION   (LOAD_DT);

CREATE INDEX IDX_SPX_DATE        ON TB_SPACEX_LAUNCHES (LAUNCH_DATE_UTC);
CREATE INDEX IDX_SPX_SUCCESS     ON TB_SPACEX_LAUNCHES (IS_SUCCESS);
CREATE INDEX IDX_SPX_ROCKET      ON TB_SPACEX_LAUNCHES (ROCKET_ID);

CREATE INDEX IDX_TEL_SAT_NAME    ON TB_SAT_TELEMETRY   (SATELLITE_NAME);
CREATE INDEX IDX_TEL_TIMESTAMP   ON TB_SAT_TELEMETRY   (TIMESTAMP_UTC);
CREATE INDEX IDX_TEL_ANOMALY     ON TB_SAT_TELEMETRY   (IS_ANOMALY, CRITICALITY);
CREATE INDEX IDX_TEL_ORBIT       ON TB_SAT_TELEMETRY   (ORBIT_TYPE);
