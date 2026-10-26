-- =============================================================================
-- DataMart S.A.S. - Repositorio analítico
-- 01. Esquemas por capa
--   staging : tablas de paso para cargas masivas (COPY), se vacían en cada run
--   dwh     : modelo estrella consultado por el negocio
--   audit   : log de rechazos y bitácora de ejecuciones del pipeline
-- Todos los scripts son idempotentes (IF NOT EXISTS / ON CONFLICT): se ejecutan al
-- crear el contenedor y de nuevo en la primera tarea del DAG.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS audit;
