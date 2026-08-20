-- Runtime writers lock the session row before touching operation relations.
-- Join that order and wait for every pre-migration writer before deciding
-- whether all dispatch-bearing launch attempts have a durable owner route.
LOCK TABLE session_authority.sessions IN EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE;

DO $writer_supervisor_state_owner_migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_claims AS launch
    WHERE launch.kind = 'writer-launch-attempt-v1'
      AND launch.state IN ('starting', 'uncertain')
  ) OR EXISTS (
    SELECT 1
    FROM session_authority.sessions AS session
    WHERE session.document #> '{launch}' IS NOT NULL
      AND session.document #> '{launch}' <> 'null'::jsonb
  ) THEN
    RAISE EXCEPTION
      'writer supervisor state-owner migration requires no active or current legacy launch attempts'
      USING
        ERRCODE = '55000',
        CONSTRAINT = 'writer_supervisor_state_owners_require_quiescent_launches';
  END IF;
END
$writer_supervisor_state_owner_migration$;

CREATE TABLE session_authority.writer_supervisor_state_owners (
  launch_attempt_id character varying(128) PRIMARY KEY,
  session_id uuid NOT NULL,
  supervisor_id character varying(128) NOT NULL,
  state_owner_id character varying(76) NOT NULL,
  bound_at timestamp with time zone NOT NULL,
  CONSTRAINT writer_supervisor_state_owners_launch_attempt_fk
    FOREIGN KEY (launch_attempt_id, session_id)
    REFERENCES session_authority.operation_claims(
      operation_id,
      session_id
    ),
  CONSTRAINT writer_supervisor_state_owners_supervisor_id_format
    CHECK (supervisor_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
  CONSTRAINT writer_supervisor_state_owners_state_owner_id_format
    CHECK (state_owner_id ~ '^state-owner:[0-9a-f]{64}$'),
  CONSTRAINT writer_supervisor_state_owners_launch_session_owner_unique
    UNIQUE (launch_attempt_id, session_id, state_owner_id)
);

CREATE FUNCTION session_authority.reject_writer_supervisor_state_owner_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $reject_writer_supervisor_state_owner_update$
BEGIN
  RAISE EXCEPTION
    'writer supervisor state-owner bindings are immutable'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'writer_supervisor_state_owners_immutable';
END
$reject_writer_supervisor_state_owner_update$;

CREATE TRIGGER writer_supervisor_state_owners_reject_update
BEFORE UPDATE ON session_authority.writer_supervisor_state_owners
FOR EACH ROW
EXECUTE FUNCTION session_authority.reject_writer_supervisor_state_owner_update();

-- Defer deletion validation so FK-ordered session teardown may remove the
-- owner row before removing its permanent operation-ID claim in the same
-- transaction. Keeping that claim while deleting and reinserting its owner
-- must never transfer recovery authority to another state root.
CREATE FUNCTION session_authority.enforce_writer_supervisor_state_owner_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_writer_supervisor_state_owner_delete$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_id_registry AS registry
    WHERE registry.operation_id = OLD.launch_attempt_id
      AND registry.session_id = OLD.session_id
  )
  THEN
    RAISE EXCEPTION
      'writer supervisor state-owner deletion requires complete operation ID teardown'
      USING
        ERRCODE = '23503',
        CONSTRAINT = 'writer_supervisor_state_owners_delete_requires_claim_teardown';
  END IF;
  RETURN OLD;
END
$enforce_writer_supervisor_state_owner_delete$;

CREATE CONSTRAINT TRIGGER writer_supervisor_state_owners_enforce_delete_teardown
AFTER DELETE ON session_authority.writer_supervisor_state_owners
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_supervisor_state_owner_delete();

-- This constraint is deferred because the current writer claim transaction
-- moves the operation to starting before it inserts the immutable owner row.
-- The commit boundary must see both writes or neither write may become
-- durable. It also fences an already-running pre-migration binary: that
-- binary can update the row, but its ownerless transaction cannot commit.
CREATE FUNCTION session_authority.enforce_writer_launch_state_owner()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_writer_launch_state_owner$
BEGIN
  IF NEW.kind <> 'writer-launch-attempt-v1'
    OR NEW.state = 'prepared'
  THEN
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE'
    AND OLD.state = 'prepared'
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

  IF NEW.state IN ('starting', 'uncertain', 'committed')
    AND EXISTS (
      SELECT 1
      FROM session_authority.writer_supervisor_state_owners AS owner
      WHERE owner.launch_attempt_id = NEW.operation_id
        AND owner.session_id = NEW.session_id
        AND owner.supervisor_id =
          NEW.request #>> '{payload,supervisor,supervisorId}'
        AND owner.bound_at >= NEW.created_at
        AND (
          (
            NEW.state = 'starting'
            AND owner.bound_at = NEW.updated_at
          )
          OR (
            NEW.state IN ('uncertain', 'committed')
            AND owner.bound_at <= NEW.updated_at
          )
        )
    )
  THEN
    RETURN NEW;
  END IF;

  RAISE EXCEPTION
    'writer launch dispatch requires an immutable state-owner binding'
    USING
      ERRCODE = '23514',
      CONSTRAINT = 'operation_claims_writer_launch_state_owner';
END
$enforce_writer_launch_state_owner$;

CREATE CONSTRAINT TRIGGER operation_claims_writer_launch_state_owner_guard
AFTER INSERT OR UPDATE ON session_authority.operation_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_writer_launch_state_owner();

CREATE TABLE session_authority.writer_supervisor_state_gc (
  terminal_operation_id character varying(128) PRIMARY KEY,
  session_id uuid NOT NULL,
  launch_attempt_id character varying(128) NOT NULL,
  state_owner_id character varying(76) NOT NULL,
  terminal_kind character varying(64) NOT NULL,
  terminal_record jsonb NOT NULL,
  terminal_record_sha256 character(64) NOT NULL,
  authorization_sha256 character(64) NOT NULL,
  authorized_at timestamp with time zone NOT NULL,
  collection_status character varying(16),
  collection_receipt_sha256 character(64),
  collected_at timestamp with time zone,
  CONSTRAINT writer_supervisor_state_gc_terminal_operation_fk
    FOREIGN KEY (terminal_operation_id, session_id)
    REFERENCES session_authority.operation_claims(
      operation_id,
      session_id
    ),
  CONSTRAINT writer_supervisor_state_gc_state_owner_fk
    FOREIGN KEY (launch_attempt_id, session_id, state_owner_id)
    REFERENCES session_authority.writer_supervisor_state_owners(
      launch_attempt_id,
      session_id,
      state_owner_id
    ),
  CONSTRAINT writer_supervisor_state_gc_terminal_kind_allowed
    CHECK (
      terminal_kind IN (
        'writer-launch-attempt-v1',
        'writer-launch-stop-v1'
      )
    ),
  CONSTRAINT writer_supervisor_state_gc_terminal_record_object
    CHECK (pg_catalog.jsonb_typeof(terminal_record) = 'object'),
  CONSTRAINT writer_supervisor_state_gc_state_owner_id_format
    CHECK (state_owner_id ~ '^state-owner:[0-9a-f]{64}$'),
  CONSTRAINT writer_supervisor_state_gc_terminal_record_sha256_format
    CHECK (terminal_record_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT writer_supervisor_state_gc_authorization_sha256_format
    CHECK (authorization_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT writer_supervisor_state_gc_collection_shape
    CHECK (
      (collection_status IS NULL) =
        (collection_receipt_sha256 IS NULL)
      AND (collection_status IS NULL) = (collected_at IS NULL)
    ),
  CONSTRAINT writer_supervisor_state_gc_collection_status_allowed
    CHECK (
      collection_status IS NULL
      OR collection_status IN ('collected', 'absent')
    ),
  CONSTRAINT writer_supervisor_state_gc_collection_receipt_sha256_format
    CHECK (
      collection_receipt_sha256 IS NULL
      OR collection_receipt_sha256 ~ '^[0-9a-f]{64}$'
    ),
  CONSTRAINT writer_supervisor_state_gc_collected_after_authorization
    CHECK (collected_at IS NULL OR collected_at >= authorized_at)
);

CREATE INDEX writer_supervisor_state_gc_pending_page
  ON session_authority.writer_supervisor_state_gc (
    state_owner_id,
    session_id,
    authorized_at,
    terminal_operation_id COLLATE pg_catalog."C"
  )
  WHERE collected_at IS NULL;

ALTER TABLE session_authority.restore_recovery_cursors
  DROP CONSTRAINT restore_recovery_cursors_lane_allowed;

ALTER TABLE session_authority.restore_recovery_cursors
  ADD COLUMN after_authorized_at timestamp with time zone,
  ADD COLUMN after_terminal_operation_id
    character varying(128) COLLATE pg_catalog."C";

ALTER TABLE session_authority.restore_recovery_cursors
  ADD CONSTRAINT restore_recovery_cursors_lane_allowed
  CHECK (
    lane IN (
      'generation',
      'activation',
      'launch-attempt',
      'current-launch',
      'supervisor-state-gc'
    )
  ),
  ADD CONSTRAINT restore_recovery_cursors_gc_position_shape
  CHECK (
    (
      lane = 'supervisor-state-gc'
      AND (
        (
          after_session_id IS NULL
          AND after_authorized_at IS NULL
          AND after_terminal_operation_id IS NULL
        )
        OR (
          after_session_id IS NOT NULL
          AND after_authorized_at IS NOT NULL
          AND after_terminal_operation_id IS NOT NULL
        )
      )
    )
    OR (
      lane <> 'supervisor-state-gc'
      AND after_authorized_at IS NULL
      AND after_terminal_operation_id IS NULL
    )
  );
