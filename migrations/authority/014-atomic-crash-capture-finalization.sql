-- Runtime writers lock the session row before touching operation relations.
-- Preserve that order while replacing the capture blocker invariants.
LOCK TABLE session_authority.sessions IN EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_id_registry IN ACCESS EXCLUSIVE MODE;

LOCK TABLE session_authority.reservations IN ACCESS EXCLUSIVE MODE;

LOCK TABLE session_authority.atomic_crash_captures IN ACCESS EXCLUSIVE MODE;

-- The protected properties are the capture operation's object identity and
-- request content, plus the reservation/session access-policy blocker. A
-- provider timestamp is only an ordering signal after the exact provider row,
-- request, result digest, fence, and preclaim identities all agree.
CREATE OR REPLACE FUNCTION session_authority.enforce_writer_fence_atomic_capture_materialization()
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
    AND OLD.kind = 'atomic-crash-capture-v1'
    AND NEW.kind = 'atomic-crash-capture-v1'
    AND NEW.operation_id IS NOT DISTINCT FROM OLD.operation_id
    AND NEW.session_id IS NOT DISTINCT FROM OLD.session_id
    AND NEW.request IS NOT DISTINCT FROM OLD.request
    AND NEW.created_at IS NOT DISTINCT FROM OLD.created_at
    AND OLD.state = 'prepared'
    AND OLD.revision = 0
    AND OLD.result IS NULL
    AND OLD.retired_at IS NULL
    AND OLD.created_at = OLD.updated_at
    AND NEW.state = 'committed'
    AND NEW.revision = 1
    AND NEW.retired_at = NEW.updated_at
    AND NEW.updated_at >= OLD.updated_at
    AND pg_catalog.jsonb_typeof(NEW.result) = 'object'
    AND (
      NEW.result - ARRAY[
        'captureResultSha256',
        'outcome',
        'resultVersion'
      ]
    ) = '{}'::pg_catalog.jsonb
    AND NEW.result ? 'captureResultSha256'
    AND NEW.result ? 'outcome'
    AND NEW.result ? 'resultVersion'
    AND NEW.result #>> '{outcome}' = 'atomic-crash-captured'
    AND NEW.result #> '{resultVersion}' = '1'::pg_catalog.jsonb
    AND NEW.result #>> '{captureResultSha256}' ~ '^[0-9a-f]{64}$'
    AND EXISTS (
      SELECT 1
      FROM session_authority.operation_id_registry AS registry
      JOIN session_authority.operation_claims AS fence
        ON fence.operation_id = registry.claimant_operation_id
       AND fence.session_id = registry.session_id
      JOIN session_authority.reservations AS capture_reservation
        ON capture_reservation.operation_id = OLD.operation_id
       AND capture_reservation.session_id = OLD.session_id
      JOIN session_authority.reservations AS fence_reservation
        ON fence_reservation.operation_id = fence.operation_id
       AND fence_reservation.session_id = fence.session_id
      JOIN session_authority.sessions AS session
        ON session.session_id = OLD.session_id
      JOIN session_authority.atomic_crash_captures AS provider
        ON provider.capture_attempt_id =
             OLD.request #>> '{payload,request,captureAttemptId}'
       AND provider.operation_id =
             OLD.request #>> '{payload,request,mutationRequest,operationId}'
       AND provider.checkpoint_id =
             OLD.request #>> '{payload,request,checkpoint,checkpointId}'
       AND provider.artifact_id =
             OLD.request #>> '{payload,request,checkpoint,artifactId}'
      WHERE registry.operation_id = OLD.operation_id
        AND registry.session_id = OLD.session_id
        AND registry.claim_type =
          'writer-fence-atomic-capture-intent-v2'
        AND registry.claimant_operation_id = fence.operation_id
        AND registry.binding = OLD.request #> '{payload}'
        AND registry.claimed_at >= fence.created_at
        AND registry.claimed_at <= fence.updated_at
        AND registry.materialized_at = fence.updated_at
        AND registry.materialized_at = OLD.created_at
        AND fence.kind = 'writer-force-fence-v1'
        AND fence.request #>> '{payload,contractVersion}' = '2'
        AND fence.request #> '{payload,atomicCapture}' = registry.binding
        AND fence.state = 'committed'
        AND fence.result #>> '{outcome}' = 'writer-fenced'
        AND fence.retired_at = fence.updated_at
        AND fence_reservation.kind = fence.kind
        AND fence_reservation.state = 'released'
        AND fence_reservation.released_at = fence.updated_at
        AND fence_reservation.updated_at = fence.updated_at
        AND capture_reservation.kind = OLD.kind
        AND capture_reservation.state = 'prepared'
        AND capture_reservation.released_at IS NULL
        AND capture_reservation.created_at = OLD.created_at
        AND capture_reservation.updated_at = OLD.updated_at
        AND capture_reservation.expected_session_revision::pg_catalog.text =
          OLD.request #>> '{expectedSession,revision}'
        AND capture_reservation.payload #> '{reservationVersion}' =
          '1'::pg_catalog.jsonb
        AND capture_reservation.payload #>> '{conflictClass}' =
          session.document #>> '{activeOperation,conflictClass}'
        AND capture_reservation.payload #>> '{requestSha256}' =
          session.document #>> '{activeOperation,requestSha256}'
        AND OLD.operation_id = OLD.request #>> '{payload,operationId}'
        AND OLD.session_id::pg_catalog.text =
          OLD.request #>> '{payload,request,storageRef,sessionId}'
        AND OLD.request #>> '{expectedSession,document,lifecycle}' =
          'DETACHED'
        AND OLD.request #> '{expectedSession,document,activeOperation}' =
          'null'::pg_catalog.jsonb
        AND OLD.request #> '{expectedSession,document,attachment}' =
          'null'::pg_catalog.jsonb
        AND OLD.request #> '{expectedSession,document,lease}' =
          'null'::pg_catalog.jsonb
        AND OLD.request #> '{expectedSession,document,launch}' =
          'null'::pg_catalog.jsonb
        AND OLD.request #>>
          '{expectedSession,document,lastOperation,operationId}' =
            fence.operation_id
        AND OLD.request #>>
          '{expectedSession,document,lastOperation,reservationId}' =
            fence_reservation.reservation_id
        AND OLD.request #>>
          '{expectedSession,document,lastOperation,kind}' = fence.kind
        AND OLD.request #>>
          '{expectedSession,document,lastOperation,state}' = fence.state
        AND OLD.request #>>
          '{expectedSession,document,lastOperation,operationRevision}' =
            fence.revision::pg_catalog.text
        AND OLD.request #>>
          '{expectedSession,document,lastOperation,requestSha256}' =
            fence_reservation.payload #>> '{requestSha256}'
        AND OLD.request #>> '{expectedSession,sessionId}' =
          fence.session_id::pg_catalog.text
        AND (
          OLD.request #>> '{expectedSession,updatedAt}'
        )::pg_catalog.timestamptz = fence.updated_at
        AND session.document #>> '{lifecycle}' = 'DETACHED'
        AND session.document #> '{lease}' = 'null'::pg_catalog.jsonb
        AND session.document #> '{attachment}' = 'null'::pg_catalog.jsonb
        AND session.document #> '{launch}' = 'null'::pg_catalog.jsonb
        AND session.document #>> '{activeOperation,operationId}' =
          OLD.operation_id
        AND session.document #>> '{activeOperation,reservationId}' =
          capture_reservation.reservation_id
        AND session.document #>> '{activeOperation,kind}' = OLD.kind
        AND session.document #>> '{activeOperation,state}' = OLD.state
        AND session.document #>> '{activeOperation,operationRevision}' =
          OLD.revision::pg_catalog.text
        AND session.document #>>
          '{activeOperation,expectedSessionRevision}' =
            OLD.request #>> '{expectedSession,revision}'
        AND (
          session.document ||
            pg_catalog.jsonb_build_object(
              'activeOperation',
              'null'::pg_catalog.jsonb
            )
        ) = OLD.request #> '{expectedSession,document}'
        AND session.revision =
          (OLD.request #>>
            '{expectedSession,revision}')::pg_catalog.int8 + 1
        AND session.created_at =
          (OLD.request #>>
            '{expectedSession,createdAt}')::pg_catalog.timestamptz
        AND session.updated_at = OLD.created_at
        AND provider.contract_version = 1
        AND provider.backend_id =
          OLD.request #>> '{payload,request,storageRef,backendId}'
        AND provider.session_id = OLD.session_id::pg_catalog.text
        AND provider.storage_id =
          OLD.request #>> '{payload,request,storageRef,storageId}'
        AND provider.source_fencing_epoch::pg_catalog.text =
          OLD.request #>>
            '{payload,request,checkpoint,sourceFencingEpoch}'
        AND provider.request_json = OLD.request #> '{payload,request}'
        AND provider.state = 'committed'
        AND provider.result_json IS NOT NULL
        AND provider.result_sha256 =
          NEW.result #>> '{captureResultSha256}'
        AND provider.claimed_at >= OLD.created_at
        AND provider.committed_at IS NOT NULL
        AND provider.committed_at <= NEW.updated_at
    )
  THEN
    RETURN NEW;
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

-- A committed version 2 writer fence has exactly two valid successors. The
-- pending branch retains the prepared active blocker while tolerating the
-- provider's absent/starting/uncertain/committed crash windows. The terminal
-- branch requires the independently committed provider row before releasing
-- the blocker into RECOVERY_REQUIRED; it does not admit a writer successor.
CREATE OR REPLACE FUNCTION session_authority.enforce_writer_fence_atomic_capture_terminal_blocker()
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
    old_session_id := OLD.session_id::pg_catalog.uuid;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_session_id := NEW.session_id::pg_catalog.uuid;
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
            AND registry.binding = capture.request #> '{payload}'
            AND registry.claimed_at >= fence.created_at
            AND registry.claimed_at <= fence.updated_at
            AND registry.materialized_at = fence.updated_at
            AND registry.materialized_at = capture.created_at
            AND capture.kind = 'atomic-crash-capture-v1'
            AND capture.operation_id =
              capture.request #>> '{payload,operationId}'
            AND capture.session_id::pg_catalog.text =
              capture.request #>>
                '{payload,request,storageRef,sessionId}'
            AND capture.created_at = fence.updated_at
            AND capture_reservation.kind = capture.kind
            AND capture_reservation.created_at = fence.updated_at
            AND capture_reservation.expected_session_revision::pg_catalog.text =
              capture.request #>> '{expectedSession,revision}'
            AND (
              capture_reservation.payload - ARRAY[
                'conflictClass',
                'requestSha256',
                'reservationVersion'
              ]
            ) = '{}'::pg_catalog.jsonb
            AND capture_reservation.payload #> '{reservationVersion}' =
              '1'::pg_catalog.jsonb
            AND capture_reservation.payload #>> '{conflictClass}' =
              'session-mutation'
            AND capture_reservation.payload #>> '{requestSha256}' ~
              '^[0-9a-f]{64}$'
            AND fence_reservation.kind = fence.kind
            AND fence_reservation.expected_session_revision::pg_catalog.text =
              fence.request #>> '{expectedSession,revision}'
            AND fence_reservation.state = 'released'
            AND fence_reservation.released_at = fence.updated_at
            AND fence_reservation.updated_at = fence.updated_at
            AND capture.request #>>
              '{expectedSession,document,lifecycle}' = 'DETACHED'
            AND capture.request #>
              '{expectedSession,document,activeOperation}' =
                'null'::pg_catalog.jsonb
            AND capture.request #>
              '{expectedSession,document,attachment}' =
                'null'::pg_catalog.jsonb
            AND capture.request #> '{expectedSession,document,lease}' =
              'null'::pg_catalog.jsonb
            AND capture.request #> '{expectedSession,document,launch}' =
              'null'::pg_catalog.jsonb
            AND capture.request #>>
              '{expectedSession,document,lastOperation,operationId}' =
                fence.operation_id
            AND capture.request #>>
              '{expectedSession,document,lastOperation,reservationId}' =
                fence_reservation.reservation_id
            AND capture.request #>>
              '{expectedSession,document,lastOperation,kind}' = fence.kind
            AND capture.request #>>
              '{expectedSession,document,lastOperation,state}' = fence.state
            AND capture.request #>>
              '{expectedSession,document,lastOperation,operationRevision}' =
                fence.revision::pg_catalog.text
            AND capture.request #>>
              '{expectedSession,document,lastOperation,requestSha256}' =
                fence_reservation.payload #>> '{requestSha256}'
            AND capture.request #>> '{expectedSession,sessionId}' =
              fence.session_id::pg_catalog.text
            AND (
              capture.request #>> '{expectedSession,updatedAt}'
            )::pg_catalog.timestamptz = fence.updated_at
            AND session.created_at =
              (capture.request #>>
                '{expectedSession,createdAt}')::pg_catalog.timestamptz
            AND (
              (
                capture.state = 'prepared'
                AND capture.revision = 0
                AND capture.result IS NULL
                AND capture.retired_at IS NULL
                AND capture.updated_at = capture.created_at
                AND capture_reservation.state = 'prepared'
                AND capture_reservation.released_at IS NULL
                AND capture_reservation.updated_at = capture.created_at
                AND session.document #>> '{lifecycle}' = 'DETACHED'
                AND session.document #> '{lease}' =
                  'null'::pg_catalog.jsonb
                AND session.document #> '{attachment}' =
                  'null'::pg_catalog.jsonb
                AND session.document #> '{launch}' =
                  'null'::pg_catalog.jsonb
                AND session.document #>>
                  '{activeOperation,operationId}' = capture.operation_id
                AND session.document #>>
                  '{activeOperation,reservationId}' =
                    capture_reservation.reservation_id
                AND session.document #>> '{activeOperation,kind}' =
                  capture.kind
                AND session.document #>> '{activeOperation,state}' =
                  capture.state
                AND session.document #>>
                  '{activeOperation,operationRevision}' =
                    capture.revision::pg_catalog.text
                AND session.document #>>
                  '{activeOperation,expectedSessionRevision}' =
                    capture.request #>> '{expectedSession,revision}'
                AND session.document #>>
                  '{activeOperation,conflictClass}' =
                    capture_reservation.payload #>> '{conflictClass}'
                AND session.document #>>
                  '{activeOperation,requestSha256}' =
                    capture_reservation.payload #>> '{requestSha256}'
                AND session.document #> '{lastOperation}' =
                  capture.request #>
                    '{expectedSession,document,lastOperation}'
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
                AND session.updated_at = fence.updated_at
                AND NOT EXISTS (
                  SELECT 1
                  FROM session_authority.atomic_crash_captures AS provider
                  WHERE (
                    provider.capture_attempt_id =
                      capture.request #>>
                        '{payload,request,captureAttemptId}'
                    OR provider.operation_id =
                      capture.request #>>
                        '{payload,request,mutationRequest,operationId}'
                    OR provider.checkpoint_id =
                      capture.request #>>
                        '{payload,request,checkpoint,checkpointId}'
                    OR provider.artifact_id =
                      capture.request #>>
                        '{payload,request,checkpoint,artifactId}'
                  )
                  AND NOT (
                    provider.capture_attempt_id =
                      capture.request #>>
                        '{payload,request,captureAttemptId}'
                    AND provider.operation_id =
                      capture.request #>>
                        '{payload,request,mutationRequest,operationId}'
                    AND provider.checkpoint_id =
                      capture.request #>>
                        '{payload,request,checkpoint,checkpointId}'
                    AND provider.artifact_id =
                      capture.request #>>
                        '{payload,request,checkpoint,artifactId}'
                    AND provider.contract_version = 1
                    AND provider.backend_id =
                      capture.request #>>
                        '{payload,request,storageRef,backendId}'
                    AND provider.session_id =
                      capture.session_id::pg_catalog.text
                    AND provider.storage_id =
                      capture.request #>>
                        '{payload,request,storageRef,storageId}'
                    AND provider.source_fencing_epoch::pg_catalog.text =
                      capture.request #>>
                        '{payload,request,checkpoint,sourceFencingEpoch}'
                    AND provider.request_json =
                      capture.request #> '{payload,request}'
                    AND provider.claimed_at >= capture.created_at
                  )
                )
              )
              OR
              (
                capture.state = 'committed'
                AND capture.revision = 1
                AND capture.retired_at = capture.updated_at
                AND pg_catalog.jsonb_typeof(capture.result) = 'object'
                AND (
                  capture.result - ARRAY[
                    'captureResultSha256',
                    'outcome',
                    'resultVersion'
                  ]
                ) = '{}'::pg_catalog.jsonb
                AND capture.result ? 'captureResultSha256'
                AND capture.result ? 'outcome'
                AND capture.result ? 'resultVersion'
                AND capture.result #>> '{outcome}' =
                  'atomic-crash-captured'
                AND capture.result #> '{resultVersion}' =
                  '1'::pg_catalog.jsonb
                AND capture.result #>> '{captureResultSha256}' ~
                  '^[0-9a-f]{64}$'
                AND capture_reservation.state = 'released'
                AND capture_reservation.released_at = capture.updated_at
                AND capture_reservation.updated_at = capture.updated_at
                AND session.document #>> '{lifecycle}' =
                  'RECOVERY_REQUIRED'
                AND session.document #> '{activeOperation}' =
                  'null'::pg_catalog.jsonb
                AND session.document #> '{lease}' =
                  'null'::pg_catalog.jsonb
                AND session.document #> '{attachment}' =
                  'null'::pg_catalog.jsonb
                AND session.document #> '{launch}' =
                  'null'::pg_catalog.jsonb
                AND (
                  (session.document #> '{lastOperation}') - ARRAY[
                    'conflictClass',
                    'expectedSessionRevision',
                    'kind',
                    'operationId',
                    'operationRevision',
                    'requestSha256',
                    'reservationId',
                    'resultSha256',
                    'state'
                  ]
                ) = '{}'::pg_catalog.jsonb
                AND session.document #>>
                  '{lastOperation,operationId}' = capture.operation_id
                AND session.document #>>
                  '{lastOperation,reservationId}' =
                    capture_reservation.reservation_id
                AND session.document #>> '{lastOperation,kind}' =
                  capture.kind
                AND session.document #>> '{lastOperation,state}' =
                  capture.state
                AND session.document #>>
                  '{lastOperation,operationRevision}' =
                    capture.revision::pg_catalog.text
                AND session.document #>>
                  '{lastOperation,expectedSessionRevision}' =
                    capture.request #>> '{expectedSession,revision}'
                AND session.document #>>
                  '{lastOperation,conflictClass}' =
                    capture_reservation.payload #>> '{conflictClass}'
                AND session.document #>>
                  '{lastOperation,requestSha256}' =
                    capture_reservation.payload #>> '{requestSha256}'
                AND session.document #>>
                  '{lastOperation,resultSha256}' =
                    pg_catalog.encode(
                      pg_catalog.sha256(
                        pg_catalog.convert_to(
                          '{"captureResultSha256":"' ||
                          capture.result #>> '{captureResultSha256}' ||
                          '","outcome":"atomic-crash-captured",' ||
                          '"resultVersion":1}',
                          'UTF8'
                        )
                      ),
                      'hex'
                    )
                AND (
                  session.document ||
                    pg_catalog.jsonb_build_object(
                      'activeOperation',
                      'null'::pg_catalog.jsonb,
                      'attachment',
                      'null'::pg_catalog.jsonb,
                      'lastOperation',
                      capture.request #>
                        '{expectedSession,document,lastOperation}',
                      'launch',
                      'null'::pg_catalog.jsonb,
                      'lease',
                      'null'::pg_catalog.jsonb,
                      'lifecycle',
                      'DETACHED'
                    )
                ) = capture.request #> '{expectedSession,document}'
                AND session.revision =
                  (capture.request #>>
                    '{expectedSession,revision}')::pg_catalog.int8 + 2
                AND session.updated_at = capture.updated_at
                AND EXISTS (
                  SELECT 1
                  FROM session_authority.atomic_crash_captures AS provider
                  WHERE provider.capture_attempt_id =
                      capture.request #>>
                        '{payload,request,captureAttemptId}'
                    AND provider.operation_id =
                      capture.request #>>
                        '{payload,request,mutationRequest,operationId}'
                    AND provider.checkpoint_id =
                      capture.request #>>
                        '{payload,request,checkpoint,checkpointId}'
                    AND provider.artifact_id =
                      capture.request #>>
                        '{payload,request,checkpoint,artifactId}'
                    AND provider.contract_version = 1
                    AND provider.backend_id =
                      capture.request #>>
                        '{payload,request,storageRef,backendId}'
                    AND provider.session_id =
                      capture.session_id::pg_catalog.text
                    AND provider.storage_id =
                      capture.request #>>
                        '{payload,request,storageRef,storageId}'
                    AND provider.source_fencing_epoch::pg_catalog.text =
                      capture.request #>>
                        '{payload,request,checkpoint,sourceFencingEpoch}'
                    AND provider.request_json =
                      capture.request #> '{payload,request}'
                    AND provider.state = 'committed'
                    AND provider.result_json IS NOT NULL
                    AND provider.result_sha256 =
                      capture.result #>> '{captureResultSha256}'
                    AND provider.claimed_at >= capture.created_at
                    AND provider.committed_at IS NOT NULL
                    AND provider.committed_at <= capture.updated_at
                )
              )
            )
        )
    )
    THEN
      RAISE EXCEPTION
        'committed writer fence requires one exact atomic capture blocker or terminal proof'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_writer_fence_atomic_capture_terminal_blocker';
    END IF;
  END LOOP;
  RETURN NULL;
END
$enforce_writer_fence_atomic_capture_terminal_blocker$;

CREATE CONSTRAINT TRIGGER atomic_crash_captures_writer_fence_terminal_blocker
AFTER INSERT OR UPDATE OR DELETE ON session_authority.atomic_crash_captures
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_fence_atomic_capture_terminal_blocker();
