CREATE TABLE session_authority.sessions (
  session_id uuid PRIMARY KEY,
  revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
  document jsonb NOT NULL CHECK (jsonb_typeof(document) = 'object'),
  created_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL,
  CHECK (updated_at >= created_at)
);

CREATE TABLE session_authority.operation_claims (
  operation_id character varying(128) PRIMARY KEY,
  session_id uuid NOT NULL REFERENCES session_authority.sessions(session_id),
  kind character varying(64) NOT NULL,
  request jsonb NOT NULL CHECK (jsonb_typeof(request) = 'object'),
  result jsonb CHECK (result IS NULL OR jsonb_typeof(result) = 'object'),
  state character varying(32) NOT NULL,
  revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL,
  retired_at timestamp with time zone,
  UNIQUE (operation_id, session_id),
  CHECK (octet_length(operation_id) BETWEEN 1 AND 128),
  CHECK (octet_length(kind) BETWEEN 1 AND 64),
  CHECK (octet_length(state) BETWEEN 1 AND 32),
  CHECK (updated_at >= created_at),
  CHECK (retired_at IS NULL OR retired_at >= created_at)
);

CREATE UNIQUE INDEX operation_claims_one_active_per_session
  ON session_authority.operation_claims (session_id)
  WHERE retired_at IS NULL;

CREATE TABLE session_authority.capture_attempt_claims (
  capture_attempt_id uuid PRIMARY KEY,
  operation_id character varying(128) NOT NULL,
  session_id uuid NOT NULL,
  binding jsonb NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
  claimed_at timestamp with time zone NOT NULL,
  UNIQUE (operation_id),
  UNIQUE (capture_attempt_id, operation_id, session_id),
  UNIQUE (capture_attempt_id, session_id),
  FOREIGN KEY (operation_id, session_id)
    REFERENCES session_authority.operation_claims(operation_id, session_id)
);

CREATE TABLE session_authority.capture_attempt_tombstones (
  capture_attempt_id uuid PRIMARY KEY,
  operation_id character varying(128) NOT NULL UNIQUE,
  session_id uuid NOT NULL,
  retired_at timestamp with time zone NOT NULL,
  tombstone jsonb NOT NULL CHECK (jsonb_typeof(tombstone) = 'object'),
  FOREIGN KEY (capture_attempt_id, operation_id, session_id)
    REFERENCES session_authority.capture_attempt_claims(
      capture_attempt_id,
      operation_id,
      session_id
    )
);

CREATE TABLE session_authority.checkpoint_catalogue (
  checkpoint_id character varying(128) PRIMARY KEY,
  session_id uuid NOT NULL,
  capture_attempt_id uuid NOT NULL UNIQUE,
  document jsonb NOT NULL CHECK (jsonb_typeof(document) = 'object'),
  committed_at timestamp with time zone NOT NULL,
  CHECK (octet_length(checkpoint_id) BETWEEN 1 AND 128),
  FOREIGN KEY (capture_attempt_id, session_id)
    REFERENCES session_authority.capture_attempt_claims(
      capture_attempt_id,
      session_id
    )
);

CREATE TABLE session_authority.reservations (
  reservation_id character varying(128) PRIMARY KEY,
  operation_id character varying(128) NOT NULL UNIQUE,
  session_id uuid NOT NULL,
  kind character varying(64) NOT NULL,
  expected_session_revision bigint NOT NULL
    CHECK (expected_session_revision >= 0),
  state character varying(32) NOT NULL,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  created_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL,
  expires_at timestamp with time zone,
  released_at timestamp with time zone,
  FOREIGN KEY (operation_id, session_id)
    REFERENCES session_authority.operation_claims(operation_id, session_id),
  CHECK (octet_length(reservation_id) BETWEEN 1 AND 128),
  CHECK (octet_length(kind) BETWEEN 1 AND 64),
  CHECK (octet_length(state) BETWEEN 1 AND 32),
  CHECK (updated_at >= created_at),
  CHECK (expires_at IS NULL OR expires_at > created_at),
  CHECK (released_at IS NULL OR released_at >= created_at)
);

CREATE UNIQUE INDEX reservations_one_active_per_session
  ON session_authority.reservations (session_id)
  WHERE released_at IS NULL;
