CREATE TABLE session_authority.writer_supervisor_state_gc (
  terminal_operation_id character varying(128) PRIMARY KEY,
  session_id uuid NOT NULL,
  launch_attempt_id character varying(128) NOT NULL,
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
  CONSTRAINT writer_supervisor_state_gc_launch_attempt_fk
    FOREIGN KEY (launch_attempt_id, session_id)
    REFERENCES session_authority.operation_claims(
      operation_id,
      session_id
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
    session_id,
    authorized_at,
    terminal_operation_id
  )
  WHERE collected_at IS NULL;

ALTER TABLE session_authority.restore_recovery_cursors
  DROP CONSTRAINT restore_recovery_cursors_lane_allowed;

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
  );
