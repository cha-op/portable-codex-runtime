CREATE TABLE session_authority.restore_recovery_cursors (
  recovery_scope_id character varying(128) NOT NULL,
  lane character varying(32) NOT NULL,
  after_session_id uuid,
  cycle bigint NOT NULL DEFAULT 0,
  revision bigint NOT NULL DEFAULT 0,
  last_transition_id uuid,
  last_request_sha256 character(64),
  updated_at timestamp with time zone NOT NULL,
  PRIMARY KEY (recovery_scope_id, lane),
  CONSTRAINT restore_recovery_cursors_scope_id_length
    CHECK (octet_length(recovery_scope_id) BETWEEN 1 AND 128),
  CONSTRAINT restore_recovery_cursors_lane_allowed
    CHECK (
      lane IN (
        'generation',
        'activation',
        'launch-attempt',
        'current-launch'
      )
    ),
  CONSTRAINT restore_recovery_cursors_cycle_nonnegative
    CHECK (cycle >= 0),
  CONSTRAINT restore_recovery_cursors_revision_nonnegative
    CHECK (revision >= 0),
  CONSTRAINT restore_recovery_cursors_cycle_within_revision
    CHECK (cycle <= revision),
  CONSTRAINT restore_recovery_cursors_transition_digest_pair
    CHECK (
      (last_transition_id IS NULL) =
      (last_request_sha256 IS NULL)
    ),
  CONSTRAINT restore_recovery_cursors_request_sha256_format
    CHECK (
      last_request_sha256 IS NULL
      OR last_request_sha256 ~ '^[0-9a-f]{64}$'
    ),
  CONSTRAINT restore_recovery_cursors_initial_shape
    CHECK (
      revision <> 0
      OR (
        after_session_id IS NULL
        AND cycle = 0
        AND last_transition_id IS NULL
        AND last_request_sha256 IS NULL
      )
    ),
  CONSTRAINT restore_recovery_cursors_progressed_shape
    CHECK (
      revision = 0
      OR (
        last_transition_id IS NOT NULL
        AND last_request_sha256 IS NOT NULL
      )
    )
);
