-- Runtime writers lock the session row before touching operation relations.
-- Preserve that order while adding the physical-fence capture handoff.
LOCK TABLE session_authority.sessions IN EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_id_registry IN ACCESS EXCLUSIVE MODE;

DO $writer_fence_atomic_capture_handoff_migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_claims
    WHERE kind = 'writer-force-fence-v1'
      AND request #>> '{payload,contractVersion}' = '2'
      AND state <> 'prepared'
  ) THEN
    RAISE EXCEPTION
      'writer fence atomic-capture handoff migration requires no legacy version 2 fence operations'
      USING ERRCODE = '55000';
  END IF;
END
$writer_fence_atomic_capture_handoff_migration$;

CREATE INDEX operation_claims_writer_fence_v2_terminal_session_idx
  ON session_authority.operation_claims (session_id)
  WHERE kind = 'writer-force-fence-v1'
    AND request #>> '{payload,contractVersion}' = '2'
    AND state = 'committed'
    AND result #>> '{outcome}' = 'writer-fenced';

ALTER TABLE session_authority.operation_id_registry
  DROP CONSTRAINT operation_id_registry_claim_type_allowed;

ALTER TABLE session_authority.operation_id_registry
  ADD CONSTRAINT operation_id_registry_claim_type_allowed
  CHECK (
    claim_type IN (
      'direct-operation',
      'restore-launch-intent-v2',
      'restore-activation-launch-intent-v1',
      'writer-stop-capture-intent-v3',
      'detached-restore-stable-plan-v1',
      'writer-fence-atomic-capture-intent-v2'
    )
  );

ALTER TABLE session_authority.operation_id_registry
  DROP CONSTRAINT operation_id_registry_claim_shape;

ALTER TABLE session_authority.operation_id_registry
  ADD CONSTRAINT operation_id_registry_claim_shape
  CHECK ((
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
        'writer-stop-capture-intent-v3',
        'writer-fence-atomic-capture-intent-v2'
      )
      AND claimant_operation_id IS NOT NULL
      AND claimant_operation_id <> operation_id
      AND binding IS NOT NULL
    )
    OR
    (
      claim_type = 'detached-restore-stable-plan-v1'
      AND claimant_operation_id IS NULL
      AND binding IS NOT NULL
      AND (
        binding - ARRAY[
          'bindingSha256',
          'contractVersion',
          'planSha256',
          'request'
        ]
      ) = '{}'::pg_catalog.jsonb
      AND binding ? 'bindingSha256'
      AND binding ? 'contractVersion'
      AND binding ? 'planSha256'
      AND binding ? 'request'
      AND binding -> 'contractVersion' = '1'::pg_catalog.jsonb
      AND pg_catalog.jsonb_typeof(binding -> 'bindingSha256') = 'string'
      AND binding ->> 'bindingSha256' ~ '^[0-9a-f]{64}$'
      AND pg_catalog.jsonb_typeof(binding -> 'planSha256') = 'string'
      AND binding ->> 'planSha256' ~ '^[0-9a-f]{64}$'
      AND pg_catalog.jsonb_typeof(binding -> 'request') = 'object'
      AND (
        materialized_at IS NULL
        OR materialized_at >= claimed_at
      )
    )
  ) IS TRUE);

CREATE FUNCTION session_authority.enforce_writer_fence_atomic_capture_id_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_writer_fence_atomic_capture_id_claim$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.kind = 'writer-force-fence-v1'
      AND OLD.request #>> '{payload,contractVersion}' = '2'
    THEN
      RAISE EXCEPTION
        'writer force-fence version 2 identity is immutable'
        USING
          ERRCODE = '55000',
          CONSTRAINT = 'operation_claims_writer_fence_v2_immutable';
    END IF;
    RETURN OLD;
  END IF;
  IF TG_OP = 'UPDATE'
    AND (
      (
        OLD.kind = 'writer-force-fence-v1'
        AND OLD.request #>> '{payload,contractVersion}' = '2'
      )
      OR
      (
        NEW.kind = 'writer-force-fence-v1'
        AND NEW.request #>> '{payload,contractVersion}' = '2'
      )
    )
    AND (
      NEW.operation_id IS DISTINCT FROM OLD.operation_id
      OR NEW.session_id IS DISTINCT FROM OLD.session_id
      OR NEW.kind IS DISTINCT FROM OLD.kind
      OR NEW.request IS DISTINCT FROM OLD.request
      OR NEW.created_at IS DISTINCT FROM OLD.created_at
      OR (
        OLD.kind = 'writer-force-fence-v1'
        AND OLD.request #>> '{payload,contractVersion}' = '2'
        AND OLD.state <> 'prepared'
        AND NEW.state = 'prepared'
      )
      OR (
        OLD.kind = 'writer-force-fence-v1'
        AND OLD.request #>> '{payload,contractVersion}' = '2'
        AND OLD.state = 'committed'
        AND NEW IS DISTINCT FROM OLD
      )
    )
  THEN
    RAISE EXCEPTION
      'writer force-fence version 2 identity is immutable'
      USING
        ERRCODE = '55000',
        CONSTRAINT = 'operation_claims_writer_fence_v2_immutable';
  END IF;
  IF NEW.kind = 'writer-force-fence-v1'
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
          NEW.request #>> '{payload,atomicCapture,operationId}'
        AND registry.session_id = NEW.session_id
        AND registry.claim_type =
          'writer-fence-atomic-capture-intent-v2'
        AND registry.claimant_operation_id = NEW.operation_id
        AND registry.binding = NEW.request #> '{payload,atomicCapture}'
        AND registry.claimed_at >= NEW.created_at
        AND registry.claimed_at <= NEW.updated_at
        AND (
          (
            NEW.state IN ('starting', 'uncertain')
            AND NEW.result IS NULL
            AND NEW.retired_at IS NULL
            AND registry.materialized_at IS NULL
            AND (
              NEW.state <> 'starting'
              OR registry.claimed_at = NEW.updated_at
            )
          )
          OR
          (
            NEW.state = 'committed'
            AND NEW.result #>> '{outcome}' = 'writer-fenced'
            AND NEW.retired_at = NEW.updated_at
            AND registry.materialized_at = NEW.updated_at
          )
          OR
          (
            NEW.state = 'committed'
            AND NEW.result #>> '{outcome}' = 'writer-blocked'
            AND NEW.revision = 3
            AND NEW.retired_at = NEW.updated_at
            AND registry.materialized_at IS NULL
          )
        )
    )
    THEN
      RAISE EXCEPTION
        'writer force-fence version 2 requires an exact durable atomic capture claim'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_writer_fence_atomic_capture_id_claim';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_writer_fence_atomic_capture_id_claim$;

CREATE TRIGGER operation_claims_enforce_writer_fence_atomic_capture_id_claim
BEFORE INSERT OR UPDATE OR DELETE ON session_authority.operation_claims
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_fence_atomic_capture_id_claim();

CREATE FUNCTION session_authority.enforce_writer_fence_atomic_capture_materialization()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_writer_fence_atomic_capture_materialization$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.kind = 'atomic-crash-capture-v1' THEN
      RAISE EXCEPTION
        'atomic crash-capture blocker is immutable'
        USING
          ERRCODE = '55000',
          CONSTRAINT = 'operation_claims_atomic_crash_capture_immutable';
    END IF;
    RETURN OLD;
  END IF;
  IF TG_OP = 'UPDATE'
    AND (
      OLD.kind = 'atomic-crash-capture-v1'
      OR NEW.kind = 'atomic-crash-capture-v1'
    )
  THEN
    RAISE EXCEPTION
      'atomic crash-capture blocker is immutable'
      USING
        ERRCODE = '55000',
        CONSTRAINT = 'operation_claims_atomic_crash_capture_immutable';
  END IF;
  IF NEW.kind = 'atomic-crash-capture-v1' THEN
    IF TG_OP <> 'INSERT'
      OR NEW.state <> 'prepared'
      OR NEW.revision <> 0
      OR NEW.result IS NOT NULL
      OR NEW.retired_at IS NOT NULL
      OR NEW.created_at <> NEW.updated_at
      OR NEW.operation_id <> NEW.request #>> '{payload,operationId}'
      OR NEW.session_id::pg_catalog.text <>
        NEW.request #>> '{payload,request,storageRef,sessionId}'
      OR NEW.request #>> '{expectedSession,document,lifecycle}' <>
        'DETACHED'
      OR NEW.request #> '{expectedSession,document,activeOperation}' <>
        'null'::pg_catalog.jsonb
      OR NEW.request #> '{expectedSession,document,attachment}' <>
        'null'::pg_catalog.jsonb
      OR NEW.request #> '{expectedSession,document,lease}' <>
        'null'::pg_catalog.jsonb
      OR NEW.request #> '{expectedSession,document,launch}' <>
        'null'::pg_catalog.jsonb
      OR NOT EXISTS (
        SELECT 1
        FROM session_authority.operation_id_registry AS registry
        JOIN session_authority.operation_claims AS fence
          ON fence.operation_id = registry.claimant_operation_id
         AND fence.session_id = registry.session_id
        WHERE registry.operation_id = NEW.operation_id
          AND registry.session_id = NEW.session_id
          AND registry.claim_type =
            'writer-fence-atomic-capture-intent-v2'
          AND registry.binding = NEW.request #> '{payload}'
          AND registry.materialized_at IS NOT NULL
          AND registry.materialized_at = NEW.created_at
          AND fence.kind = 'writer-force-fence-v1'
          AND fence.request #>> '{payload,contractVersion}' = '2'
          AND fence.request #> '{payload,atomicCapture}' =
            NEW.request #> '{payload}'
          AND fence.state = 'committed'
          AND fence.result #>> '{outcome}' = 'writer-fenced'
          AND fence.updated_at = registry.materialized_at
          AND fence.retired_at = fence.updated_at
          AND NEW.request #>>
            '{expectedSession,document,lastOperation,operationId}' =
              fence.operation_id
          AND NEW.request #>>
            '{expectedSession,document,lastOperation,kind}' =
              fence.kind
          AND NEW.request #>> '{expectedSession,sessionId}' =
              fence.session_id::pg_catalog.text
      )
    THEN
      RAISE EXCEPTION
        'atomic crash-capture blocker requires one exact committed writer fence handoff'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_writer_fence_atomic_capture_materialization';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_writer_fence_atomic_capture_materialization$;

CREATE TRIGGER operation_claims_enforce_writer_fence_atomic_capture_materialization
BEFORE INSERT OR UPDATE OR DELETE ON session_authority.operation_claims
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_fence_atomic_capture_materialization();

-- The protected property is the durable identity and access-policy role of
-- the prepared capture blocker, not timestamp stability by itself. Immutable
-- fence/capture rows preserve the proof and blocker identities. The reverse
-- check below joins those identities and canonical bindings to the prepared
-- operation/reservation, released fence reservation, and exact session
-- pointers. Their shared timestamp proves only the intended same-transaction
-- handoff after those stronger signals agree.
CREATE FUNCTION session_authority.enforce_writer_fence_atomic_capture_terminal_blocker()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_writer_fence_atomic_capture_terminal_blocker$
DECLARE
  affected_session_id uuid;
  old_session_id uuid;
  new_session_id uuid;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_session_id := OLD.session_id;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_session_id := NEW.session_id;
  END IF;

  FOR affected_session_id IN
    SELECT candidate.session_id
    FROM (
      SELECT old_session_id AS session_id
      UNION
      SELECT new_session_id AS session_id
    ) AS candidate
    WHERE candidate.session_id IS NOT NULL
  LOOP
    IF EXISTS (
      SELECT 1
      FROM session_authority.operation_claims AS fence
      WHERE fence.session_id = affected_session_id
        AND fence.kind = 'writer-force-fence-v1'
        AND fence.request #>> '{payload,contractVersion}' = '2'
        AND fence.state = 'committed'
        AND fence.result #>> '{outcome}' = 'writer-fenced'
        AND NOT EXISTS (
          SELECT 1
          FROM session_authority.operation_id_registry AS registry
          JOIN session_authority.operation_claims AS capture
            ON capture.operation_id = registry.operation_id
           AND capture.session_id = registry.session_id
          JOIN session_authority.reservations AS capture_reservation
            ON capture_reservation.operation_id = capture.operation_id
           AND capture_reservation.session_id = capture.session_id
          JOIN session_authority.reservations AS fence_reservation
            ON fence_reservation.operation_id = fence.operation_id
           AND fence_reservation.session_id = fence.session_id
          JOIN session_authority.sessions AS session
            ON session.session_id = fence.session_id
          WHERE registry.operation_id =
              fence.request #>> '{payload,atomicCapture,operationId}'
            AND registry.session_id = fence.session_id
            AND registry.claim_type =
              'writer-fence-atomic-capture-intent-v2'
            AND registry.claimant_operation_id = fence.operation_id
            AND registry.binding = fence.request #> '{payload,atomicCapture}'
            AND registry.materialized_at = fence.updated_at
            AND capture.kind = 'atomic-crash-capture-v1'
            AND capture.request #> '{payload}' = registry.binding
            AND capture.state = 'prepared'
            AND capture.revision = 0
            AND capture.result IS NULL
            AND capture.retired_at IS NULL
            AND capture.created_at = fence.updated_at
            AND capture.updated_at = fence.updated_at
            AND capture_reservation.kind = capture.kind
            AND capture_reservation.state = 'prepared'
            AND capture_reservation.released_at IS NULL
            AND capture_reservation.created_at = fence.updated_at
            AND capture_reservation.updated_at = fence.updated_at
            AND capture_reservation.expected_session_revision::pg_catalog.text =
              capture.request #>> '{expectedSession,revision}'
            AND capture_reservation.payload #>> '{conflictClass}' =
              session.document #>> '{activeOperation,conflictClass}'
            AND capture_reservation.payload #>> '{requestSha256}' =
              session.document #>> '{activeOperation,requestSha256}'
            AND fence_reservation.kind = fence.kind
            AND fence_reservation.expected_session_revision::pg_catalog.text =
              fence.request #>> '{expectedSession,revision}'
            AND fence_reservation.state = 'released'
            AND fence_reservation.released_at = fence.updated_at
            AND fence_reservation.updated_at = fence.updated_at
            AND session.document #>> '{lifecycle}' = 'DETACHED'
            AND session.document #> '{lease}' = 'null'::pg_catalog.jsonb
            AND session.document #> '{attachment}' = 'null'::pg_catalog.jsonb
            AND session.document #> '{launch}' = 'null'::pg_catalog.jsonb
            AND session.document #>> '{activeOperation,operationId}' =
              capture.operation_id
            AND session.document #>> '{activeOperation,reservationId}' =
              capture_reservation.reservation_id
            AND session.document #>> '{activeOperation,kind}' = capture.kind
            AND session.document #>> '{activeOperation,state}' = capture.state
            AND session.document #>>
              '{activeOperation,operationRevision}' =
                capture.revision::pg_catalog.text
            AND session.document #>>
              '{activeOperation,expectedSessionRevision}' =
                capture.request #>> '{expectedSession,revision}'
            AND session.document #> '{lastOperation}' =
              capture.request #> '{expectedSession,document,lastOperation}'
            AND session.document #>> '{lastOperation,operationId}' =
              fence.operation_id
            AND session.document #>> '{lastOperation,reservationId}' =
              fence_reservation.reservation_id
            AND session.document #>> '{lastOperation,kind}' = fence.kind
            AND session.document #>> '{lastOperation,state}' = fence.state
            AND session.document #>> '{lastOperation,operationRevision}' =
              fence.revision::pg_catalog.text
            AND session.document #>> '{lastOperation,requestSha256}' =
              fence_reservation.payload #>> '{requestSha256}'
            AND (
              session.document ||
                pg_catalog.jsonb_build_object(
                  'activeOperation',
                  'null'::pg_catalog.jsonb
                )
            ) = capture.request #> '{expectedSession,document}'
            AND session.revision =
              (capture.request #>>
                '{expectedSession,revision}')::pg_catalog.int8 + 1
            AND session.created_at =
              (capture.request #>>
                '{expectedSession,createdAt}')::pg_catalog.timestamptz
            AND session.updated_at = fence.updated_at
            AND (
              capture.request #>> '{expectedSession,updatedAt}'
            )::pg_catalog.timestamptz = fence.updated_at
        )
    )
    THEN
      RAISE EXCEPTION
        'committed writer fence requires one exact prepared atomic capture blocker'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_writer_fence_atomic_capture_terminal_blocker';
    END IF;
  END LOOP;
  RETURN NULL;
END
$enforce_writer_fence_atomic_capture_terminal_blocker$;

CREATE CONSTRAINT TRIGGER operation_claims_writer_fence_atomic_capture_terminal_blocker
AFTER INSERT OR UPDATE OR DELETE ON session_authority.operation_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_fence_atomic_capture_terminal_blocker();

CREATE CONSTRAINT TRIGGER operation_registry_writer_fence_capture_terminal_blocker
AFTER INSERT OR UPDATE OR DELETE ON session_authority.operation_id_registry
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_fence_atomic_capture_terminal_blocker();

CREATE CONSTRAINT TRIGGER reservations_writer_fence_capture_terminal_blocker
AFTER INSERT OR UPDATE OR DELETE ON session_authority.reservations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_fence_atomic_capture_terminal_blocker();

CREATE CONSTRAINT TRIGGER sessions_writer_fence_capture_terminal_blocker
AFTER INSERT OR UPDATE OR DELETE ON session_authority.sessions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_fence_atomic_capture_terminal_blocker();
