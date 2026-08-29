-- Durable provider-neutral catalogue for atomic crash-prefix capture.
-- Four independently unique opaque identities prevent any attempt,
-- operation, checkpoint, or artifact from being rebound to another row.
CREATE TABLE session_authority.atomic_crash_captures (
  capture_attempt_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  operation_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  checkpoint_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  artifact_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  contract_version integer NOT NULL,
  backend_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  session_id character varying(36) COLLATE pg_catalog."C" NOT NULL,
  storage_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  source_fencing_epoch numeric(20, 0) NOT NULL,
  request_json jsonb NOT NULL,
  request_sha256 character varying(64) COLLATE pg_catalog."C" NOT NULL,
  provider_binding jsonb NOT NULL,
  provider_binding_json text COLLATE pg_catalog."C" NOT NULL,
  provider_binding_sha256 character varying(64) COLLATE pg_catalog."C" NOT NULL,
  state character varying(10) COLLATE pg_catalog."C" NOT NULL,
  result_json jsonb,
  result_sha256 character varying(64) COLLATE pg_catalog."C",
  claimed_at timestamp with time zone NOT NULL,
  uncertain_at timestamp with time zone,
  committed_at timestamp with time zone,
  CONSTRAINT atomic_crash_captures_pkey PRIMARY KEY (capture_attempt_id),
  CONSTRAINT atomic_crash_captures_operation_id_uniq UNIQUE (operation_id),
  CONSTRAINT atomic_crash_captures_checkpoint_id_uniq UNIQUE (checkpoint_id),
  CONSTRAINT atomic_crash_captures_artifact_id_uniq UNIQUE (artifact_id),
  CONSTRAINT atomic_crash_captures_contract_version CHECK (contract_version = 1),
  CONSTRAINT atomic_crash_captures_capture_attempt_id CHECK ((
    octet_length(capture_attempt_id) BETWEEN 1 AND 128
    AND capture_attempt_id ~ '^[A-Za-z0-9]'
    AND capture_attempt_id !~ '[^A-Za-z0-9._:-]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_operation_id CHECK ((
    octet_length(operation_id) BETWEEN 1 AND 128
    AND operation_id ~ '^[A-Za-z0-9]'
    AND operation_id !~ '[^A-Za-z0-9._:-]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_checkpoint_id CHECK ((
    octet_length(checkpoint_id) BETWEEN 1 AND 128
    AND checkpoint_id ~ '^[A-Za-z0-9]'
    AND checkpoint_id !~ '[^A-Za-z0-9._:-]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_artifact_id CHECK ((
    octet_length(artifact_id) BETWEEN 1 AND 128
    AND artifact_id ~ '^[A-Za-z0-9]'
    AND artifact_id !~ '[^A-Za-z0-9._:-]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_backend_id CHECK ((
    octet_length(backend_id) BETWEEN 1 AND 128
    AND backend_id ~ '^[A-Za-z0-9]'
    AND backend_id !~ '[^A-Za-z0-9._:-]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_session_id CHECK ((
    octet_length(session_id) = 36
    AND session_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_storage_id CHECK ((
    octet_length(storage_id) BETWEEN 1 AND 128
    AND storage_id ~ '^[A-Za-z0-9]'
    AND storage_id !~ '[^A-Za-z0-9._:-]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_source_epoch CHECK (
    source_fencing_epoch BETWEEN 1 AND 18446744073709551615
  ),
  CONSTRAINT atomic_crash_captures_request_shape CHECK ((
    pg_catalog.jsonb_typeof(request_json) = 'object'
    AND pg_catalog.pg_column_size(request_json) BETWEEN 2 AND 262144
    AND request_json #>> '{contractVersion}' = '1'
    AND request_json #>> '{captureAttemptId}' = capture_attempt_id
    AND request_json #>> '{mutationRequest,operationId}' = operation_id
    AND request_json #>> '{checkpoint,checkpointId}' = checkpoint_id
    AND request_json #>> '{checkpoint,artifactId}' = artifact_id
    AND request_json #>> '{storageRef,backendId}' = backend_id
    AND request_json #>> '{storageRef,sessionId}' = session_id
    AND request_json #>> '{storageRef,storageId}' = storage_id
    AND request_json #>> '{checkpoint,sourceFencingEpoch}' =
      source_fencing_epoch::pg_catalog.text
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_request_sha256 CHECK ((
    octet_length(request_sha256) = 64
    AND request_sha256 !~ '[^0-9a-f]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_provider_binding_shape CHECK ((
    pg_catalog.jsonb_typeof(provider_binding) = 'object'
    AND pg_catalog.octet_length(
      pg_catalog.convert_to(provider_binding_json, 'UTF8')
    ) BETWEEN 2 AND 65536
    AND provider_binding_json::pg_catalog.jsonb = provider_binding
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_provider_sha256 CHECK ((
    octet_length(provider_binding_sha256) = 64
    AND provider_binding_sha256 !~ '[^0-9a-f]'
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_state CHECK (
    state IN ('starting', 'uncertain', 'committed')
  ),
  CONSTRAINT atomic_crash_captures_transition_times CHECK ((
    (
      state = 'starting'
      AND uncertain_at IS NULL
      AND committed_at IS NULL
    )
    OR (
      state = 'uncertain'
      AND uncertain_at IS NOT NULL
      AND uncertain_at >= claimed_at
      AND committed_at IS NULL
    )
    OR (
      state = 'committed'
      AND committed_at IS NOT NULL
      AND committed_at >= claimed_at
      AND (
        uncertain_at IS NULL
        OR (
          uncertain_at >= claimed_at
          AND committed_at >= uncertain_at
        )
      )
    )
  ) IS TRUE),
  CONSTRAINT atomic_crash_captures_result_shape CHECK ((
    (
      state IN ('starting', 'uncertain')
      AND result_json IS NULL
      AND result_sha256 IS NULL
    )
    OR (
      state = 'committed'
      AND result_json IS NOT NULL
      AND result_sha256 IS NOT NULL
      AND pg_catalog.jsonb_typeof(result_json) = 'object'
      AND pg_catalog.pg_column_size(result_json) BETWEEN 2 AND 131072
      AND octet_length(result_sha256) = 64
      AND result_sha256 !~ '[^0-9a-f]'
      AND result_json #>> '{contractVersion}' = '1'
      AND result_json #>> '{status}' = 'committed'
      AND result_json #>> '{captureAttemptId}' = capture_attempt_id
      AND result_json #>> '{operationId}' = operation_id
      AND result_json #>> '{checkpointId}' = checkpoint_id
      AND result_json #>> '{artifactId}' = artifact_id
      AND result_json #>> '{backendId}' = backend_id
      AND result_json #>> '{sessionId}' = session_id
      AND result_json #>> '{storageId}' = storage_id
      AND result_json #>> '{sourceFencingEpoch}' =
        source_fencing_epoch::pg_catalog.text
    )
  ) IS TRUE)
);

CREATE FUNCTION session_authority.enforce_atomic_crash_capture_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_atomic_crash_capture_insert$
BEGIN
  IF NEW.state = 'starting'
    AND NEW.result_json IS NULL
    AND NEW.result_sha256 IS NULL
    AND NEW.claimed_at IS NULL
    AND NEW.uncertain_at IS NULL
    AND NEW.committed_at IS NULL
  THEN
    NEW.claimed_at := pg_catalog.transaction_timestamp();
    RETURN NEW;
  END IF;

  RAISE EXCEPTION 'atomic crash-capture inserts require starting state'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'atomic_crash_captures_insert_starting_only';
END
$enforce_atomic_crash_capture_insert$;

CREATE TRIGGER atomic_crash_captures_insert_guard
BEFORE INSERT ON session_authority.atomic_crash_captures
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_atomic_crash_capture_insert();

CREATE FUNCTION session_authority.enforce_atomic_crash_capture_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_atomic_crash_capture_update$
BEGIN
  IF NEW.capture_attempt_id IS NOT DISTINCT FROM OLD.capture_attempt_id
    AND NEW.operation_id IS NOT DISTINCT FROM OLD.operation_id
    AND NEW.checkpoint_id IS NOT DISTINCT FROM OLD.checkpoint_id
    AND NEW.artifact_id IS NOT DISTINCT FROM OLD.artifact_id
    AND NEW.contract_version IS NOT DISTINCT FROM OLD.contract_version
    AND NEW.backend_id IS NOT DISTINCT FROM OLD.backend_id
    AND NEW.session_id IS NOT DISTINCT FROM OLD.session_id
    AND NEW.storage_id IS NOT DISTINCT FROM OLD.storage_id
    AND NEW.source_fencing_epoch IS NOT DISTINCT FROM OLD.source_fencing_epoch
    AND NEW.request_json IS NOT DISTINCT FROM OLD.request_json
    AND NEW.request_sha256 IS NOT DISTINCT FROM OLD.request_sha256
    AND NEW.provider_binding IS NOT DISTINCT FROM OLD.provider_binding
    AND NEW.provider_binding_json IS NOT DISTINCT FROM OLD.provider_binding_json
    AND NEW.provider_binding_sha256 IS NOT DISTINCT FROM OLD.provider_binding_sha256
    AND NEW.claimed_at IS NOT DISTINCT FROM OLD.claimed_at
    AND NEW.uncertain_at IS NOT DISTINCT FROM OLD.uncertain_at
    AND NEW.committed_at IS NOT DISTINCT FROM OLD.committed_at
    AND OLD.result_json IS NULL
    AND OLD.result_sha256 IS NULL
    AND (
      (
        OLD.state = 'starting'
        AND NEW.state = 'uncertain'
        AND NEW.result_json IS NULL
        AND NEW.result_sha256 IS NULL
      )
      OR (
        OLD.state IN ('starting', 'uncertain')
        AND NEW.state = 'committed'
        AND NEW.result_json IS NOT NULL
        AND NEW.result_sha256 IS NOT NULL
      )
    )
  THEN
    IF NEW.state = 'uncertain' THEN
      NEW.uncertain_at := pg_catalog.transaction_timestamp();
    ELSE
      NEW.committed_at := pg_catalog.transaction_timestamp();
    END IF;
    RETURN NEW;
  END IF;

  RAISE EXCEPTION 'atomic crash-capture row transition is immutable'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'atomic_crash_captures_immutable_transition';
END
$enforce_atomic_crash_capture_update$;

CREATE TRIGGER atomic_crash_captures_update_guard
BEFORE UPDATE ON session_authority.atomic_crash_captures
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_atomic_crash_capture_update();

CREATE FUNCTION session_authority.reject_atomic_crash_capture_removal()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $reject_atomic_crash_capture_removal$
BEGIN
  RAISE EXCEPTION 'atomic crash-capture rows are permanent'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'atomic_crash_captures_permanent';
END
$reject_atomic_crash_capture_removal$;

CREATE TRIGGER atomic_crash_captures_delete_guard
BEFORE DELETE ON session_authority.atomic_crash_captures
FOR EACH ROW
EXECUTE FUNCTION session_authority.reject_atomic_crash_capture_removal();

CREATE TRIGGER atomic_crash_captures_truncate_guard
BEFORE TRUNCATE ON session_authority.atomic_crash_captures
FOR EACH STATEMENT
EXECUTE FUNCTION session_authority.reject_atomic_crash_capture_removal();
