-- Runtime writers lock the session row before touching operation relations.
-- Preserve that order while changing the registry and operation triggers.
LOCK TABLE session_authority.sessions IN EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_id_registry IN ACCESS EXCLUSIVE MODE;

DO $writer_stop_capture_handoff_migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_claims
    WHERE kind = 'writer-launch-stop-v1'
      AND request #>> '{payload,contractVersion}' = '3'
  ) THEN
    RAISE EXCEPTION
      'writer stop capture handoff migration requires no legacy version 3 stop operations'
      USING ERRCODE = '55000';
  END IF;
END
$writer_stop_capture_handoff_migration$;

ALTER TABLE session_authority.operation_id_registry
  DROP CONSTRAINT operation_id_registry_claim_type_allowed;

ALTER TABLE session_authority.operation_id_registry
  ADD CONSTRAINT operation_id_registry_claim_type_allowed
  CHECK (
    claim_type IN (
      'direct-operation',
      'restore-launch-intent-v2',
      'restore-activation-launch-intent-v1',
      'writer-stop-capture-intent-v3'
    )
  );

ALTER TABLE session_authority.operation_id_registry
  DROP CONSTRAINT operation_id_registry_claim_shape;

ALTER TABLE session_authority.operation_id_registry
  ADD CONSTRAINT operation_id_registry_claim_shape
  CHECK (
    (
      claim_type = 'direct-operation'
      AND claimant_operation_id IS NULL
      AND binding IS NULL
      AND materialized_at IS NOT NULL
      AND materialized_at = claimed_at
    )
    OR
    (
      claim_type IN (
        'restore-launch-intent-v2',
        'restore-activation-launch-intent-v1',
        'writer-stop-capture-intent-v3'
      )
      AND claimant_operation_id IS NOT NULL
      AND claimant_operation_id <> operation_id
      AND binding IS NOT NULL
    )
  );

CREATE FUNCTION session_authority.enforce_writer_stop_capture_id_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_writer_stop_capture_id_claim$
BEGIN
  IF NEW.kind = 'writer-launch-stop-v1'
    AND NEW.request #>> '{payload,contractVersion}' = '3'
    AND NEW.state <> 'prepared'
  THEN
    IF TG_OP = 'UPDATE' THEN
      IF OLD.state = 'prepared'
        AND OLD.result IS NULL
        AND OLD.retired_at IS NULL
        AND NEW.state = 'committed'
        AND NEW.result #>> '{outcome}' = 'cancelled-before-dispatch'
        AND NEW.revision = OLD.revision + 1
        AND NEW.retired_at = NEW.updated_at
        AND NEW.kind = OLD.kind
        AND NEW.request = OLD.request
        AND NEW.session_id = OLD.session_id
      THEN
        RETURN NEW;
      END IF;
    END IF;
    IF NOT EXISTS (
      SELECT 1
      FROM session_authority.operation_id_registry AS registry
      WHERE registry.operation_id =
          NEW.request #>> '{payload,captureIntent,admission,request,operationId}'
        AND registry.session_id = NEW.session_id
        AND registry.claim_type = 'writer-stop-capture-intent-v3'
        AND registry.claimant_operation_id = NEW.operation_id
        AND registry.binding = NEW.request #> '{payload,captureIntent}'
        AND (
          (
            NEW.state IN ('starting', 'uncertain')
            AND registry.materialized_at IS NULL
          )
          OR
          (
            NEW.state = 'committed'
            AND NEW.result #>> '{outcome}' = 'writer-launch-stopped'
            AND registry.materialized_at IS NOT NULL
          )
        )
    )
    THEN
      RAISE EXCEPTION
        'writer stop version 3 dispatch requires a durable capture operation ID claim'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_writer_stop_capture_id_claim';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_writer_stop_capture_id_claim$;

CREATE TRIGGER operation_claims_enforce_writer_stop_capture_id_claim
BEFORE INSERT OR UPDATE ON session_authority.operation_claims
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_stop_capture_id_claim();

CREATE FUNCTION session_authority.enforce_writer_stop_capture_materialization()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_writer_stop_capture_materialization$
BEGIN
  IF NEW.kind = 'checkpoint-capture-v1'
    AND EXISTS (
      SELECT 1
      FROM session_authority.operation_id_registry AS registry
      WHERE registry.operation_id = NEW.operation_id
        AND registry.session_id = NEW.session_id
        AND registry.claim_type = 'writer-stop-capture-intent-v3'
    )
  THEN
    IF NOT EXISTS (
      SELECT 1
      FROM session_authority.operation_id_registry AS registry
      JOIN session_authority.operation_claims AS stop
        ON stop.operation_id = registry.claimant_operation_id
       AND stop.session_id = registry.session_id
      WHERE registry.operation_id = NEW.operation_id
        AND registry.session_id = NEW.session_id
        AND registry.claim_type = 'writer-stop-capture-intent-v3'
        AND registry.binding = NEW.request #> '{payload}'
        AND registry.materialized_at IS NOT NULL
        AND stop.kind = 'writer-launch-stop-v1'
        AND stop.request #>> '{payload,contractVersion}' = '3'
        AND stop.request #> '{payload,captureIntent}' =
          NEW.request #> '{payload}'
        AND stop.state = 'committed'
        AND stop.result #>> '{outcome}' = 'writer-launch-stopped'
    )
    THEN
      RAISE EXCEPTION
        'checkpoint capture handoff requires one materialized writer stop claim'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_writer_stop_capture_materialization';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_writer_stop_capture_materialization$;

CREATE TRIGGER operation_claims_enforce_writer_stop_capture_materialization
BEFORE INSERT OR UPDATE ON session_authority.operation_claims
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_stop_capture_materialization();
