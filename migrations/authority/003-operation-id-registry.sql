LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE;

DO $operation_id_registry_migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_claims
    WHERE kind = 'restore-destination-generation-v1'
      AND request #>> '{payload,contractVersion}' = '2'
      -- Only prepared proves that external restore dispatch has not begun.
      AND state <> 'prepared'
  ) THEN
    RAISE EXCEPTION
      'operation ID registry migration requires quiescent legacy restore v2 operations'
      USING ERRCODE = '55000';
  END IF;
END
$operation_id_registry_migration$;

CREATE TABLE session_authority.operation_id_registry (
  operation_id character varying(128) PRIMARY KEY,
  session_id uuid NOT NULL REFERENCES session_authority.sessions(session_id),
  claim_type character varying(64) NOT NULL,
  claimant_operation_id character varying(128),
  binding jsonb,
  claimed_at timestamp with time zone NOT NULL,
  materialized_at timestamp with time zone,
  CONSTRAINT operation_id_registry_operation_session_unique
    UNIQUE (operation_id, session_id),
  CONSTRAINT operation_id_registry_claimant_unique
    UNIQUE (claimant_operation_id),
  CONSTRAINT operation_id_registry_operation_id_length
    CHECK (octet_length(operation_id) BETWEEN 1 AND 128),
  CONSTRAINT operation_id_registry_claim_type_allowed
    CHECK (claim_type IN ('direct-operation', 'restore-launch-intent-v2')),
  CONSTRAINT operation_id_registry_claimant_operation_id_length
    CHECK (
      claimant_operation_id IS NULL
      OR octet_length(claimant_operation_id) BETWEEN 1 AND 128
    ),
  CONSTRAINT operation_id_registry_binding_object
    CHECK (binding IS NULL OR jsonb_typeof(binding) = 'object'),
  CONSTRAINT operation_id_registry_materialized_at_order
    CHECK (materialized_at IS NULL OR materialized_at >= claimed_at),
  CONSTRAINT operation_id_registry_claim_shape
    CHECK (
      (
        claim_type = 'direct-operation'
        AND claimant_operation_id IS NULL
        AND binding IS NULL
        AND materialized_at = claimed_at
      )
      OR
      (
        claim_type = 'restore-launch-intent-v2'
        AND claimant_operation_id IS NOT NULL
        AND claimant_operation_id <> operation_id
        AND binding IS NOT NULL
      )
    ),
  CONSTRAINT operation_id_registry_claimant_operation_fk
    FOREIGN KEY (claimant_operation_id, session_id)
    REFERENCES session_authority.operation_claims(operation_id, session_id)
);

INSERT INTO session_authority.operation_id_registry (
  operation_id,
  session_id,
  claim_type,
  claimant_operation_id,
  binding,
  claimed_at,
  materialized_at
)
SELECT
  operation_id,
  session_id,
  'direct-operation',
  NULL,
  NULL,
  created_at,
  created_at
FROM session_authority.operation_claims;

ALTER TABLE session_authority.operation_claims
  ADD CONSTRAINT operation_claims_operation_id_registry_fk
  FOREIGN KEY (operation_id, session_id)
  REFERENCES session_authority.operation_id_registry(operation_id, session_id);

CREATE FUNCTION session_authority.enforce_restore_v2_launch_id_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_restore_v2_launch_id_claim$
BEGIN
  IF NEW.kind = 'restore-destination-generation-v1'
    AND NEW.request #>> '{payload,contractVersion}' = '2'
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
        AND registry.claim_type = 'restore-launch-intent-v2'
        AND registry.claimant_operation_id = NEW.operation_id
        AND registry.binding = NEW.request #> '{payload,launchIntent}'
        AND registry.materialized_at IS NULL
    )
    THEN
      RAISE EXCEPTION
        'restore v2 dispatch requires a durable launch operation ID claim'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_restore_v2_launch_id_claim';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_restore_v2_launch_id_claim$;

CREATE TRIGGER operation_claims_enforce_restore_v2_launch_id_claim
BEFORE INSERT OR UPDATE ON session_authority.operation_claims
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_restore_v2_launch_id_claim();
