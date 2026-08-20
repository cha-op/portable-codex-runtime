-- Contract version 3 adds a PostgreSQL-indexed permanent operation history.
-- Existing valid version 2 head values remain valid; writers must not create
-- version 3 heads until the matching adapter rollout gate is explicitly open.
-- Convert the version 2 bpchar checksums before opening version 3. The USING
-- casts remove bpchar padding, then PostgreSQL revalidates the existing format
-- constraints so any historically short value blocks this migration.
ALTER TABLE session_authority.filesystem_image_provider_heads
  ALTER COLUMN base_head_checksum
    TYPE character varying(64) COLLATE pg_catalog."C"
    USING base_head_checksum::character varying(64),
  ALTER COLUMN checkpoint_checksum
    TYPE character varying(64) COLLATE pg_catalog."C"
    USING checkpoint_checksum::character varying(64),
  ALTER COLUMN last_checksum
    TYPE character varying(64) COLLATE pg_catalog."C"
    USING last_checksum::character varying(64);

ALTER TABLE session_authority.filesystem_image_provider_heads
  DROP CONSTRAINT filesystem_image_provider_heads_contract_version_supported;

ALTER TABLE session_authority.filesystem_image_provider_heads
  ADD CONSTRAINT filesystem_image_provider_heads_contract_version_supported
    CHECK (contract_version IN (2, 3));

-- Canonical record bytes are the authority. PostgreSQL stores their caller-
-- verified SHA-256 digests but does not decode or reserialize the records.
CREATE TABLE session_authority.filesystem_image_provider_operations (
  provider_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  anchor_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  operation_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  record_contract_version integer NOT NULL,
  state character varying(16) COLLATE pg_catalog."C" NOT NULL,
  kind character varying(32) COLLATE pg_catalog."C" NOT NULL,
  storage_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  prepared_state_revision numeric(20, 0) NOT NULL,
  prepared_checksum character varying(64) COLLATE pg_catalog."C" NOT NULL,
  prepared_record_bytes bytea NOT NULL,
  prepared_record_sha256 character varying(64) COLLATE pg_catalog."C" NOT NULL,
  committed_state_revision numeric(20, 0),
  committed_checksum_provenance character varying(32) COLLATE pg_catalog."C",
  committed_checksum character varying(64) COLLATE pg_catalog."C",
  committed_record_bytes bytea,
  committed_record_sha256 character varying(64) COLLATE pg_catalog."C",
  PRIMARY KEY (provider_id, anchor_id, operation_id),
  CONSTRAINT filesystem_image_provider_operations_head_fk
    FOREIGN KEY (provider_id, anchor_id)
    REFERENCES session_authority.filesystem_image_provider_heads(
      provider_id,
      anchor_id
    ),
  CONSTRAINT filesystem_image_provider_operations_provider_id_format
    CHECK ((
      octet_length(provider_id) BETWEEN 1 AND 128
      AND provider_id ~ '^[A-Za-z0-9]'
      AND provider_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_operations_anchor_id_format
    CHECK ((
      octet_length(anchor_id) BETWEEN 1 AND 128
      AND anchor_id ~ '^[A-Za-z0-9]'
      AND anchor_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_operations_operation_id_format
    CHECK ((
      octet_length(operation_id) BETWEEN 1 AND 128
      AND operation_id ~ '^[A-Za-z0-9]'
      AND operation_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_operations_storage_id_format
    CHECK ((
      octet_length(storage_id) BETWEEN 1 AND 128
      AND storage_id ~ '^[A-Za-z0-9]'
      AND storage_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_operations_kind_format
    CHECK ((
      octet_length(kind) BETWEEN 1 AND 32
      AND kind ~ '^[A-Za-z0-9]'
      AND kind !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_operations_kind_allowed
    CHECK (
      kind IN (
        'provision',
        'attach',
        'detach',
        'destroy',
        'checkpoint',
        'restore',
        'restore-attach'
      )
    ),
  CONSTRAINT filesystem_image_provider_operations_contract_version_supported
    CHECK (record_contract_version = 1),
  CONSTRAINT filesystem_image_provider_operations_state_allowed
    CHECK (state IN ('prepared', 'committed')),
  CONSTRAINT filesystem_image_provider_operations_prepared_revision_bounded
    CHECK (
      prepared_state_revision BETWEEN 1 AND 18446744073709551615
    ),
  CONSTRAINT filesystem_image_provider_operations_prepared_checksum_format
    CHECK ((
      octet_length(prepared_checksum) = 64
      AND prepared_checksum !~ '[^0-9a-f]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_operations_prepared_bytes_bounded
    CHECK (octet_length(prepared_record_bytes) BETWEEN 1 AND 4194304),
  CONSTRAINT filesystem_image_provider_operations_prepared_sha256_format
    CHECK ((
      octet_length(prepared_record_sha256) = 64
      AND prepared_record_sha256 !~ '[^0-9a-f]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_operations_committed_record_shape
    CHECK ((
      (
        state = 'prepared'
        AND committed_state_revision IS NULL
        AND committed_checksum_provenance IS NULL
        AND committed_checksum IS NULL
        AND committed_record_bytes IS NULL
        AND committed_record_sha256 IS NULL
      )
      OR (
        state = 'committed'
        AND committed_state_revision IS NOT NULL
        AND committed_checksum_provenance IS NOT NULL
        AND committed_record_bytes IS NOT NULL
        AND committed_record_sha256 IS NOT NULL
        AND committed_state_revision > prepared_state_revision
        AND committed_state_revision <= 18446744073709551615
        AND (
          (
            committed_checksum_provenance = 'indexed-frame-v1'
            AND committed_checksum IS NOT NULL
            AND octet_length(committed_checksum) = 64
            AND committed_checksum !~ '[^0-9a-f]'
          )
          OR (
            committed_checksum_provenance = 'unavailable-adopted-v2'
            AND committed_checksum IS NULL
          )
        )
        AND octet_length(committed_record_bytes) BETWEEN 1 AND 4194304
        AND octet_length(committed_record_sha256) = 64
        AND committed_record_sha256 !~ '[^0-9a-f]'
      )
    ) IS TRUE)
);

CREATE UNIQUE INDEX filesystem_image_provider_operations_one_prepared_storage
  ON session_authority.filesystem_image_provider_operations (
    provider_id,
    anchor_id,
    storage_id
  )
  WHERE state = 'prepared';

CREATE INDEX filesystem_image_provider_operations_state_storage_idx
  ON session_authority.filesystem_image_provider_operations (
    provider_id,
    anchor_id,
    state,
    storage_id COLLATE pg_catalog."C"
  );

CREATE INDEX filesystem_image_provider_operations_prepared_revision_idx
  ON session_authority.filesystem_image_provider_operations (
    provider_id,
    anchor_id,
    prepared_state_revision,
    operation_id COLLATE pg_catalog."C"
  );

-- Every history row enters through its prepared prefix. Adoption may insert
-- and then commit old history in one transaction, but it may not manufacture a
-- committed suffix without first passing through the same one-way edge.
CREATE FUNCTION session_authority.enforce_filesystem_image_provider_operation_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_filesystem_image_provider_operation_insert$
BEGIN
  IF NEW.state = 'prepared'
    AND NEW.committed_state_revision IS NULL
    AND NEW.committed_checksum_provenance IS NULL
    AND NEW.committed_checksum IS NULL
    AND NEW.committed_record_bytes IS NULL
    AND NEW.committed_record_sha256 IS NULL
  THEN
    RETURN NEW;
  END IF;

  RAISE EXCEPTION
    'filesystem image provider operation inserts require prepared history'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'filesystem_image_provider_operations_insert_prepared_only';
END
$enforce_filesystem_image_provider_operation_insert$;

CREATE TRIGGER filesystem_image_provider_operations_insert_guard
BEFORE INSERT ON session_authority.filesystem_image_provider_operations
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_filesystem_image_provider_operation_insert();

-- A permanent operation record has one mutable edge: its exact prepared row
-- may acquire a committed suffix once. Neither identity nor the canonical
-- prepared bytes may be rebound during that transition.
CREATE FUNCTION session_authority.enforce_filesystem_image_provider_operation_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_filesystem_image_provider_operation_update$
BEGIN
  IF OLD.state = 'prepared'
    AND NEW.state = 'committed'
    AND NEW.provider_id IS NOT DISTINCT FROM OLD.provider_id
    AND NEW.anchor_id IS NOT DISTINCT FROM OLD.anchor_id
    AND NEW.operation_id IS NOT DISTINCT FROM OLD.operation_id
    AND NEW.record_contract_version IS NOT DISTINCT FROM OLD.record_contract_version
    AND NEW.kind IS NOT DISTINCT FROM OLD.kind
    AND NEW.storage_id IS NOT DISTINCT FROM OLD.storage_id
    AND NEW.prepared_state_revision IS NOT DISTINCT FROM OLD.prepared_state_revision
    AND NEW.prepared_checksum IS NOT DISTINCT FROM OLD.prepared_checksum
    AND NEW.prepared_record_bytes IS NOT DISTINCT FROM OLD.prepared_record_bytes
    AND NEW.prepared_record_sha256 IS NOT DISTINCT FROM OLD.prepared_record_sha256
  THEN
    IF NEW.committed_checksum_provenance = 'unavailable-adopted-v2'
      AND NOT EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_heads AS head
        WHERE head.provider_id = NEW.provider_id
          AND head.anchor_id = NEW.anchor_id
          AND head.contract_version = 3
          AND NEW.committed_state_revision <= head.checkpoint_state_revision
      )
    THEN
      RAISE EXCEPTION
        'adopted filesystem image provider history requires a covering version 3 checkpoint'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'filesystem_image_provider_operations_adopted_v3_cut';
    END IF;
    RETURN NEW;
  END IF;

  RAISE EXCEPTION
    'filesystem image provider operation updates require an exact prepared-to-committed transition'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'filesystem_image_provider_operations_prepared_to_committed_only';
END
$enforce_filesystem_image_provider_operation_update$;

CREATE TRIGGER filesystem_image_provider_operations_update_guard
BEFORE UPDATE ON session_authority.filesystem_image_provider_operations
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_filesystem_image_provider_operation_update();

-- Defer the permanence check so teardown may remove operation rows before its
-- parent head in FK order. A standalone delete remains forbidden at commit.
CREATE FUNCTION session_authority.enforce_filesystem_image_provider_operation_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_filesystem_image_provider_operation_delete$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.filesystem_image_provider_heads AS head
    WHERE head.provider_id = OLD.provider_id
      AND head.anchor_id = OLD.anchor_id
  )
  THEN
    RAISE EXCEPTION
      'filesystem image provider operation deletion requires complete anchor teardown'
      USING
        ERRCODE = '23503',
        CONSTRAINT = 'filesystem_image_provider_operations_delete_requires_teardown';
  END IF;
  RETURN OLD;
END
$enforce_filesystem_image_provider_operation_delete$;

CREATE CONSTRAINT TRIGGER filesystem_image_provider_operations_delete_guard
AFTER DELETE ON session_authority.filesystem_image_provider_operations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_filesystem_image_provider_operation_delete();

-- Row triggers cannot make permanent history survive TRUNCATE, so reject the
-- statement itself before PostgreSQL can bypass the row-level delete guard.
CREATE FUNCTION session_authority.reject_filesystem_image_provider_operation_truncate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $reject_filesystem_image_provider_operation_truncate$
BEGIN
  RAISE EXCEPTION
    'filesystem image provider operation history cannot be truncated'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'filesystem_image_provider_operations_truncate_forbidden';
END
$reject_filesystem_image_provider_operation_truncate$;

CREATE TRIGGER filesystem_image_provider_operations_truncate_guard
BEFORE TRUNCATE ON session_authority.filesystem_image_provider_operations
FOR EACH STATEMENT
EXECUTE FUNCTION session_authority.reject_filesystem_image_provider_operation_truncate();
