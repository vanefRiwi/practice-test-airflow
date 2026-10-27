-- =============================================================================
-- DataMart S.A.S. - Analytical warehouse
-- 01. One schema per layer
--   staging : landing tables for bulk loads (COPY), emptied on every run
--   dwh     : star schema queried by the business
--   audit   : rejection log and pipeline run log
-- Every script is idempotent (IF NOT EXISTS / ON CONFLICT): they run when the
-- container is created and again in the first task of the DAG.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS audit;
