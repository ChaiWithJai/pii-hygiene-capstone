-- ---------------------------------------------------------------------------
-- Least-privilege roles for the local warehouse.
-- Mirrors the Snowflake RBAC design (INGEST / TRANSFORM / AGENT / READ /
-- REVIEWER). Passwords are local-development only and safe to publish.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pii_admin') THEN
        CREATE ROLE pii_admin LOGIN PASSWORD 'pii_admin_pw';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'streamlit_reader') THEN
        CREATE ROLE streamlit_reader LOGIN PASSWORD 'streamlit_reader_pw';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reviewer') THEN
        CREATE ROLE reviewer LOGIN PASSWORD 'reviewer_pw';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agent_service') THEN
        CREATE ROLE agent_service LOGIN PASSWORD 'agent_service_pw';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow LOGIN PASSWORD 'airflow' CREATEDB;
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE pii_lab TO pii_admin;
-- Compose's default 64 MiB /dev/shm cannot support scale-run parallel hashes.
ALTER ROLE pii_admin IN DATABASE pii_lab SET max_parallel_workers_per_gather = 0;
ALTER ROLE pii_admin IN DATABASE pii_lab SET work_mem = '64MB';
GRANT CONNECT ON DATABASE pii_lab TO streamlit_reader, reviewer, agent_service;
GRANT CREATE, USAGE ON SCHEMA raw, staging, intermediate, marts, quarantine, governance TO pii_admin;
GRANT ALL ON ALL TABLES IN SCHEMA raw, staging, intermediate, marts, quarantine, governance TO pii_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA raw, staging, intermediate, marts, quarantine, governance TO pii_admin;

-- Read-only analytics role.
GRANT USAGE ON SCHEMA raw, staging, intermediate, marts, quarantine, governance TO streamlit_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts, quarantine, governance TO streamlit_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts, quarantine, governance TO reviewer;
ALTER DEFAULT PRIVILEGES FOR ROLE pii_admin IN SCHEMA marts, quarantine, governance
    GRANT SELECT ON TABLES TO streamlit_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE pii_admin IN SCHEMA marts, quarantine, governance
    GRANT SELECT ON TABLES TO reviewer;

-- Reviewer role may only mutate the review-queue status columns.
GRANT USAGE ON SCHEMA marts, quarantine, governance TO reviewer;
GRANT SELECT, UPDATE (status, reviewed_by, reviewed_at, review_note)
    ON governance.agent_review_queue TO reviewer;

-- Agent service role: read marts, append audit/queue rows.
GRANT USAGE ON SCHEMA marts, governance TO agent_service;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO agent_service;
ALTER DEFAULT PRIVILEGES FOR ROLE pii_admin IN SCHEMA marts
    GRANT SELECT ON TABLES TO agent_service;
GRANT SELECT, INSERT ON governance.agent_review_queue TO agent_service;
GRANT SELECT, INSERT ON governance.agent_action_log TO agent_service;
GRANT SELECT ON governance.dbt_run_metadata TO agent_service;
