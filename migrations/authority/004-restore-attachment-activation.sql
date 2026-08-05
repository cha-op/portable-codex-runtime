LOCK TABLE session_authority.sessions IN EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_id_registry IN ACCESS EXCLUSIVE MODE;

DO $restore_attachment_activation_migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_claims
    WHERE kind = 'restore-attachment-activation-v1'
  ) THEN
    RAISE EXCEPTION
      'restore attachment activation migration requires no legacy operations of the new kind'
      USING ERRCODE = '55000';
  END IF;
END
$restore_attachment_activation_migration$;

ALTER TABLE session_authority.operation_id_registry
  DROP CONSTRAINT operation_id_registry_claim_type_allowed;

ALTER TABLE session_authority.operation_id_registry
  ADD CONSTRAINT operation_id_registry_claim_type_allowed
  CHECK (
    claim_type IN (
      'direct-operation',
      'restore-launch-intent-v2',
      'restore-activation-launch-intent-v1'
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
        'restore-activation-launch-intent-v1'
      )
      AND claimant_operation_id IS NOT NULL
      AND claimant_operation_id <> operation_id
      AND binding IS NOT NULL
    )
  );

CREATE FUNCTION session_authority.enforce_restore_activation_launch_id_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_restore_activation_launch_id_claim$
BEGIN
  IF NEW.kind = 'restore-attachment-activation-v1'
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
          NEW.request #>> '{payload,launchIntent,launchAttemptId}'
        AND registry.session_id = NEW.session_id
        AND registry.claim_type =
          'restore-activation-launch-intent-v1'
        AND registry.claimant_operation_id = NEW.operation_id
        AND registry.binding = NEW.request #> '{payload,launchIntent}'
        AND registry.materialized_at IS NULL
    )
    THEN
      RAISE EXCEPTION
        'restore attachment activation dispatch requires a durable launch operation ID claim'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_restore_activation_launch_id_claim';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_restore_activation_launch_id_claim$;

CREATE TRIGGER operation_claims_enforce_restore_activation_launch_id_claim
BEFORE INSERT OR UPDATE ON session_authority.operation_claims
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_restore_activation_launch_id_claim();
