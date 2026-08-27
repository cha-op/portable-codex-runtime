-- Version 3 adoption is a one-time, transaction-bound conversion. The
-- manifest identifier is derived by the adapter from the complete canonical
-- version 2 state. PostgreSQL supplies the xid so a copied identifier or head
-- token cannot reopen the legacy import path in a later transaction.
ALTER TABLE session_authority.filesystem_image_provider_heads
  ADD COLUMN operation_index_adoption_id
    character varying(64) COLLATE pg_catalog."C",
  ADD COLUMN operation_index_adoption_xid xid8,
  ADD COLUMN operation_index_progress_xid xid8,
  ADD CONSTRAINT fs_image_heads_adoption_id_format
    CHECK ((
      operation_index_adoption_id IS NULL
      OR (
        octet_length(operation_index_adoption_id) = 64
        AND operation_index_adoption_id !~ '[^0-9a-f]'
      )
    ) IS TRUE),
  ADD CONSTRAINT fs_image_heads_adoption_pair
    CHECK ((operation_index_adoption_id IS NULL) =
      (operation_index_adoption_xid IS NULL)),
  ADD CONSTRAINT fs_image_heads_adoption_v3_only
    CHECK (
      operation_index_adoption_id IS NULL
      OR contract_version = 3
    ),
  ADD CONSTRAINT fs_image_heads_stored_non_genesis
    CHECK (
      state_revision BETWEEN 1 AND 18446744073709551615
    );

ALTER TABLE session_authority.filesystem_image_provider_operations
  ADD COLUMN adoption_id character varying(64) COLLATE pg_catalog."C",
  ADD CONSTRAINT fs_image_operations_adoption_id_format
    CHECK ((
      adoption_id IS NULL
      OR (
        octet_length(adoption_id) = 64
        AND adoption_id !~ '[^0-9a-f]'
      )
    ) IS TRUE);

-- One permanent lifecycle row serializes every head insertion against history
-- retirement through a real unique-index conflict. A SELECT-only tombstone
-- check would have a READ COMMITTED race with an uncommitted teardown.
CREATE TABLE session_authority.filesystem_image_provider_anchor_lifecycle (
  provider_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  anchor_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  retired_xid xid8,
  PRIMARY KEY (provider_id, anchor_id),
  CONSTRAINT fs_image_anchor_lifecycle_provider_id_format
    CHECK ((
      octet_length(provider_id) BETWEEN 1 AND 128
      AND provider_id ~ '^[A-Za-z0-9]'
      AND provider_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT fs_image_anchor_lifecycle_anchor_id_format
    CHECK ((
      octet_length(anchor_id) BETWEEN 1 AND 128
      AND anchor_id ~ '^[A-Za-z0-9]'
      AND anchor_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE)
);

INSERT INTO session_authority.filesystem_image_provider_anchor_lifecycle
  (provider_id, anchor_id, retired_xid)
SELECT provider_id, anchor_id, NULL
FROM session_authority.filesystem_image_provider_heads;

CREATE FUNCTION session_authority.enforce_fs_image_anchor_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_fs_image_anchor_lifecycle$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.retired_xid IS NULL AND pg_trigger_depth() = 2 THEN
      RETURN NEW;
    END IF;
  ELSIF TG_OP = 'UPDATE' THEN
    IF pg_trigger_depth() = 2
      AND NEW.provider_id IS NOT DISTINCT FROM OLD.provider_id
      AND NEW.anchor_id IS NOT DISTINCT FROM OLD.anchor_id
      AND (
        NEW.retired_xid IS NOT DISTINCT FROM OLD.retired_xid
        OR OLD.retired_xid IS NULL
          AND NEW.retired_xid = pg_current_xact_id()
          AND NOT EXISTS (
            SELECT 1
            FROM session_authority.filesystem_image_provider_heads AS head
            WHERE head.provider_id = OLD.provider_id
              AND head.anchor_id = OLD.anchor_id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM session_authority.filesystem_image_provider_operations AS operation
            WHERE operation.provider_id = OLD.provider_id
              AND operation.anchor_id = OLD.anchor_id
          )
      )
    THEN
      RETURN NEW;
    END IF;
  END IF;

  RAISE EXCEPTION 'filesystem image provider anchor lifecycle is immutable'
    USING ERRCODE = '55000', CONSTRAINT = 'fs_image_anchor_lifecycle_immutable';
END
$enforce_fs_image_anchor_lifecycle$;

CREATE TRIGGER fs_image_anchor_lifecycle_row_guard
BEFORE INSERT OR UPDATE OR DELETE
ON session_authority.filesystem_image_provider_anchor_lifecycle
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_fs_image_anchor_lifecycle();

CREATE TRIGGER fs_image_anchor_lifecycle_truncate_guard
BEFORE TRUNCATE
ON session_authority.filesystem_image_provider_anchor_lifecycle
FOR EACH STATEMENT
EXECUTE FUNCTION session_authority.enforce_fs_image_anchor_lifecycle();

ALTER TABLE session_authority.filesystem_image_provider_operations
  DROP CONSTRAINT filesystem_image_provider_operations_committed_record_shape;

ALTER TABLE session_authority.filesystem_image_provider_operations
  ADD CONSTRAINT fs_image_operations_committed_shape
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
            AND adoption_id IS NOT NULL
          )
        )
        AND octet_length(committed_record_bytes) BETWEEN 1 AND 4194304
        AND octet_length(committed_record_sha256) = 64
        AND committed_record_sha256 !~ '[^0-9a-f]'
      )
    ) IS TRUE);

-- Event revisions are globally unique within one anchor even though prepared
-- and committed revisions occupy separate columns. These indexes and the
-- visible-row guards provide early rejection; the event registry below is the
-- unique-index serialization point for concurrent cross-column claims.
CREATE UNIQUE INDEX fs_image_operations_prepared_revision_uniq
  ON session_authority.filesystem_image_provider_operations (
    provider_id,
    anchor_id,
    prepared_state_revision
  );

CREATE UNIQUE INDEX fs_image_operations_committed_revision_uniq
  ON session_authority.filesystem_image_provider_operations (
    provider_id,
    anchor_id,
    committed_state_revision
  )
  WHERE state = 'committed';

DO $validate_fs_image_operation_revision_cross_unique$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.filesystem_image_provider_operations AS prepared
    JOIN session_authority.filesystem_image_provider_operations AS committed
      ON committed.provider_id = prepared.provider_id
      AND committed.anchor_id = prepared.anchor_id
      AND committed.state = 'committed'
      AND committed.committed_state_revision = prepared.prepared_state_revision
  )
  THEN
    RAISE EXCEPTION 'filesystem image provider event revisions overlap'
      USING
        ERRCODE = '23505',
        CONSTRAINT = 'fs_image_operations_revision_cross_unique';
  END IF;
END
$validate_fs_image_operation_revision_cross_unique$;

CREATE TABLE session_authority.filesystem_image_provider_operation_events (
  provider_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  anchor_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  operation_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  phase character varying(9) COLLATE pg_catalog."C" NOT NULL,
  event_revision numeric(20, 0) NOT NULL,
  CONSTRAINT fs_image_operation_events_revision_pkey
    PRIMARY KEY (provider_id, anchor_id, event_revision),
  CONSTRAINT fs_image_operation_events_identity_phase_uniq
    UNIQUE (provider_id, anchor_id, operation_id, phase),
  CONSTRAINT fs_image_operation_events_phase
    CHECK (phase IN ('prepared', 'committed')),
  CONSTRAINT fs_image_operation_events_revision
    CHECK (event_revision BETWEEN 1 AND 18446744073709551615),
  CONSTRAINT fs_image_operation_events_operation_fk
    FOREIGN KEY (provider_id, anchor_id, operation_id)
    REFERENCES session_authority.filesystem_image_provider_operations
      (provider_id, anchor_id, operation_id)
    ON DELETE CASCADE
);

-- UNION ALL deliberately lets the registry primary key reject a preexisting
-- prepared/committed collision instead of hiding it during migration.
INSERT INTO session_authority.filesystem_image_provider_operation_events
  (provider_id, anchor_id, operation_id, phase, event_revision)
SELECT provider_id, anchor_id, operation_id, 'prepared', prepared_state_revision
FROM session_authority.filesystem_image_provider_operations
UNION ALL
SELECT provider_id, anchor_id, operation_id, 'committed', committed_state_revision
FROM session_authority.filesystem_image_provider_operations
WHERE state = 'committed';

-- A non-null v10 marker already claimed complete indexed coverage. Preserve
-- only those claims that the newly serialized registry proves are exact.
DO $validate_fs_image_existing_event_cover$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.filesystem_image_provider_heads AS head
    CROSS JOIN LATERAL (
      SELECT
        coalesce(sum(1::numeric), 0) AS event_count,
        min(event.event_revision) AS first_revision,
        max(event.event_revision) AS last_revision
      FROM session_authority.filesystem_image_provider_operation_events AS event
      WHERE event.provider_id = head.provider_id
        AND event.anchor_id = head.anchor_id
    ) AS coverage
    WHERE head.operation_index_state_revision IS NOT NULL
      AND (
        head.operation_index_state_revision = 0
        OR coverage.event_count <> head.operation_index_state_revision
        OR head.operation_index_state_revision > 0
          AND (
            coverage.first_revision <> 1
            OR coverage.last_revision <> head.operation_index_state_revision
          )
      )
  )
  THEN
    RAISE EXCEPTION
      'indexed filesystem image provider event coverage is incomplete'
      USING
        ERRCODE = '23514',
        CONSTRAINT = 'fs_image_operation_events_existing_cover';
  END IF;
END
$validate_fs_image_existing_event_cover$;

CREATE FUNCTION session_authority.enforce_fs_image_operation_event()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_fs_image_operation_event$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF pg_trigger_depth() = 2
      AND EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_operations AS operation
        WHERE operation.provider_id = NEW.provider_id
          AND operation.anchor_id = NEW.anchor_id
          AND operation.operation_id = NEW.operation_id
          AND (
            NEW.phase = 'prepared'
              AND NEW.event_revision = operation.prepared_state_revision
            OR NEW.phase = 'committed'
              AND operation.state = 'committed'
              AND NEW.event_revision = operation.committed_state_revision
          )
      )
    THEN
      RETURN NEW;
    END IF;
  ELSIF TG_OP = 'DELETE' THEN
    IF pg_trigger_depth() = 2
      AND NOT EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_operations AS operation
        WHERE operation.provider_id = OLD.provider_id
          AND operation.anchor_id = OLD.anchor_id
          AND operation.operation_id = OLD.operation_id
      )
    THEN
      RETURN OLD;
    END IF;
  END IF;

  RAISE EXCEPTION 'filesystem image provider operation event is immutable'
    USING ERRCODE = '55000', CONSTRAINT = 'fs_image_operation_events_immutable';
END
$enforce_fs_image_operation_event$;

CREATE TRIGGER fs_image_operation_events_row_guard
BEFORE INSERT OR UPDATE OR DELETE
ON session_authority.filesystem_image_provider_operation_events
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_fs_image_operation_event();

CREATE TRIGGER fs_image_operation_events_truncate_guard
BEFORE TRUNCATE
ON session_authority.filesystem_image_provider_operation_events
FOR EACH STATEMENT
EXECUTE FUNCTION session_authority.enforce_fs_image_operation_event();

-- Inserts may either be native prepared prefixes or complete legacy rows in
-- the exact adoption transaction recorded on their parent head.
CREATE OR REPLACE FUNCTION session_authority.enforce_filesystem_image_provider_operation_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_filesystem_image_provider_operation_insert$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.filesystem_image_provider_operations AS operation
    WHERE operation.provider_id = NEW.provider_id
      AND operation.anchor_id = NEW.anchor_id
      AND operation.state = 'committed'
      AND operation.committed_state_revision = NEW.prepared_state_revision
  )
    OR NEW.committed_state_revision IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_operations AS operation
        WHERE operation.provider_id = NEW.provider_id
          AND operation.anchor_id = NEW.anchor_id
          AND operation.prepared_state_revision = NEW.committed_state_revision
      )
  THEN
    RAISE EXCEPTION 'filesystem image provider event revisions overlap'
      USING
        ERRCODE = '23505',
        CONSTRAINT = 'fs_image_operations_revision_cross_unique';
  END IF;

  IF NEW.adoption_id IS NULL
    AND NEW.state = 'prepared'
    AND NEW.committed_state_revision IS NULL
    AND NEW.committed_checksum_provenance IS NULL
    AND NEW.committed_checksum IS NULL
    AND NEW.committed_record_bytes IS NULL
    AND NEW.committed_record_sha256 IS NULL
    AND EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_heads AS head
      WHERE head.provider_id = NEW.provider_id
        AND head.anchor_id = NEW.anchor_id
        AND NEW.prepared_state_revision <= head.state_revision
    )
  THEN
    RETURN NEW;
  END IF;

  IF NEW.adoption_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_heads AS head
      WHERE head.provider_id = NEW.provider_id
        AND head.anchor_id = NEW.anchor_id
        AND head.contract_version = 3
        AND head.operation_index_adoption_id = NEW.adoption_id
        AND head.operation_index_adoption_xid = pg_current_xact_id()
        AND NEW.prepared_state_revision <= head.state_revision
        AND (
          NEW.committed_state_revision IS NULL
          OR NEW.committed_state_revision <= head.checkpoint_state_revision
        )
    )
  THEN
    RETURN NEW;
  END IF;

  RAISE EXCEPTION
    'filesystem image provider operation insert is outside its permitted path'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'fs_image_operations_insert_path';
END
$enforce_filesystem_image_provider_operation_insert$;

-- A prepared row may acquire one native indexed suffix. Its adoption tag, if
-- it came from a version 2 checkpoint, remains immutable across that edge.
CREATE OR REPLACE FUNCTION session_authority.enforce_filesystem_image_provider_operation_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_filesystem_image_provider_operation_update$
BEGIN
  IF OLD.state = 'prepared'
    AND NEW.state = 'committed'
    AND NEW.committed_checksum_provenance = 'indexed-frame-v1'
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
    AND NEW.adoption_id IS NOT DISTINCT FROM OLD.adoption_id
    AND EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_heads AS head
      WHERE head.provider_id = NEW.provider_id
        AND head.anchor_id = NEW.anchor_id
        AND NEW.committed_state_revision <= head.state_revision
    )
    AND NOT EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_operations AS operation
      WHERE operation.provider_id = NEW.provider_id
        AND operation.anchor_id = NEW.anchor_id
        AND operation.operation_id <> NEW.operation_id
        AND operation.prepared_state_revision = NEW.committed_state_revision
    )
  THEN
    RETURN NEW;
  END IF;

  RAISE EXCEPTION
    'filesystem image provider operation update is not an exact native commit'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'fs_image_operations_native_commit_only';
END
$enforce_filesystem_image_provider_operation_update$;

CREATE FUNCTION session_authority.claim_fs_image_operation_events()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $claim_fs_image_operation_events$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO session_authority.filesystem_image_provider_operation_events
      (provider_id, anchor_id, operation_id, phase, event_revision)
    VALUES (
      NEW.provider_id,
      NEW.anchor_id,
      NEW.operation_id,
      'prepared',
      NEW.prepared_state_revision
    );
    IF NEW.state = 'committed' THEN
      INSERT INTO session_authority.filesystem_image_provider_operation_events
        (provider_id, anchor_id, operation_id, phase, event_revision)
      VALUES (
        NEW.provider_id,
        NEW.anchor_id,
        NEW.operation_id,
        'committed',
        NEW.committed_state_revision
      );
    END IF;
  ELSIF OLD.state = 'prepared' AND NEW.state = 'committed' THEN
    INSERT INTO session_authority.filesystem_image_provider_operation_events
      (provider_id, anchor_id, operation_id, phase, event_revision)
    VALUES (
      NEW.provider_id,
      NEW.anchor_id,
      NEW.operation_id,
      'committed',
      NEW.committed_state_revision
    );
  END IF;
  RETURN NEW;
END
$claim_fs_image_operation_events$;

CREATE TRIGGER fs_image_operations_event_claim
AFTER INSERT OR UPDATE
ON session_authority.filesystem_image_provider_operations
FOR EACH ROW
EXECUTE FUNCTION session_authority.claim_fs_image_operation_events();

CREATE OR REPLACE FUNCTION session_authority.enforce_filesystem_image_provider_operation_delete()
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

  IF EXISTS (
    SELECT 1
    FROM session_authority.filesystem_image_provider_operations AS operation
    WHERE operation.provider_id = OLD.provider_id
      AND operation.anchor_id = OLD.anchor_id
  )
  THEN
    RAISE EXCEPTION
      'filesystem image provider operation deletion requires complete history teardown'
      USING
        ERRCODE = '23503',
        CONSTRAINT = 'fs_image_operations_delete_requires_complete_history';
  END IF;

  UPDATE session_authority.filesystem_image_provider_anchor_lifecycle
  SET retired_xid = pg_current_xact_id()
  WHERE provider_id = OLD.provider_id
    AND anchor_id = OLD.anchor_id
    AND retired_xid IS NULL;

  IF NOT FOUND
    AND NOT EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_anchor_lifecycle AS lifecycle
      WHERE lifecycle.provider_id = OLD.provider_id
        AND lifecycle.anchor_id = OLD.anchor_id
        AND lifecycle.retired_xid = pg_current_xact_id()
    )
  THEN
    RAISE EXCEPTION 'filesystem image provider anchor lifecycle is invalid'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_anchor_lifecycle_invalid';
  END IF;

  RETURN OLD;
END
$enforce_filesystem_image_provider_operation_delete$;

CREATE FUNCTION session_authority.enforce_fs_image_head_adoption()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_fs_image_head_adoption$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.operation_index_progress_xid IS NOT NULL THEN
      RAISE EXCEPTION
        'filesystem image provider progress xid is database managed'
        USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_progress_xid_managed';
    END IF;
    IF NEW.operation_index_adoption_id IS NOT NULL
      OR NEW.operation_index_adoption_xid IS NOT NULL
    THEN
      RAISE EXCEPTION
        'filesystem image provider adoption requires an existing version 2 head'
        USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_adoption_insert';
    END IF;
    IF NEW.state_revision = 0 THEN
      RAISE EXCEPTION
        'filesystem image provider genesis cannot have a stored head row'
        USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_initial_progress';
    END IF;
    IF NEW.operation_index_state_revision IS NOT NULL
      AND NOT (
        NEW.anchor_revision = 1
          AND NEW.generation = 0
          AND NEW.state_revision = 1
          AND NEW.base_head_checksum IS NULL
          AND NEW.checkpoint_state_revision = 0
          AND NEW.checkpoint_frame_count = 0
          AND NEW.checkpoint_checksum IS NULL
          AND NEW.checkpoint_bytes = 0
          AND NEW.frame_count = 1
          AND NEW.last_checksum IS NOT NULL
          AND NEW.ledger_bytes > 0
      )
    THEN
      RAISE EXCEPTION
        'indexed filesystem image provider head insertion is not a first append'
        USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_initial_progress';
    END IF;
    NEW.operation_index_progress_xid := pg_current_xact_id();
    RETURN NEW;
  END IF;

  IF NEW.provider_id IS DISTINCT FROM OLD.provider_id
    OR NEW.anchor_id IS DISTINCT FROM OLD.anchor_id
  THEN
    RAISE EXCEPTION
      'filesystem image provider head identity is immutable'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_identity_immutable';
  END IF;

  IF OLD.operation_index_adoption_xid IS NOT NULL
    AND OLD.operation_index_adoption_xid = pg_current_xact_id()
  THEN
    RAISE EXCEPTION
      'filesystem image provider adoption head cannot advance again in its adoption transaction'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_adoption_same_xact_update';
  END IF;

  IF OLD.operation_index_progress_xid = pg_current_xact_id() THEN
    RAISE EXCEPTION
      'filesystem image provider head cannot mutate twice in one transaction'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_same_xact_update';
  END IF;

  IF NEW.operation_index_progress_xid IS DISTINCT FROM OLD.operation_index_progress_xid
  THEN
    RAISE EXCEPTION
      'filesystem image provider progress xid is database managed'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_progress_xid_managed';
  END IF;
  NEW.operation_index_progress_xid := pg_current_xact_id();

  IF NEW.contract_version IS DISTINCT FROM OLD.contract_version THEN
    IF OLD.contract_version = 2
      AND NEW.contract_version = 3
      AND OLD.operation_index_adoption_id IS NULL
      AND OLD.operation_index_adoption_xid IS NULL
      AND NEW.operation_index_adoption_id IS NOT NULL
      AND NEW.operation_index_adoption_xid IS NULL
      AND NEW.provider_id IS NOT DISTINCT FROM OLD.provider_id
      AND NEW.anchor_id IS NOT DISTINCT FROM OLD.anchor_id
      AND OLD.state_revision > 0
      AND NEW.state_revision > 0
      AND NEW.anchor_revision = OLD.anchor_revision + 1
      AND NEW.generation = OLD.generation + 1
      AND NEW.state_revision = OLD.state_revision
      AND NEW.checkpoint_state_revision = OLD.state_revision
      AND NEW.operation_index_state_revision = OLD.state_revision
      AND NEW.base_head_checksum IS NOT NULL
      AND NEW.checkpoint_checksum IS NOT NULL
      AND NEW.checkpoint_frame_count BETWEEN 2 AND 4294967295
      AND NEW.checkpoint_bytes BETWEEN 1 AND 9007199254740991
      AND NEW.frame_count = 0
      AND NEW.last_checksum = NEW.checkpoint_checksum
      AND NEW.ledger_bytes = 0
    THEN
      NEW.operation_index_adoption_xid := pg_current_xact_id();
      RETURN NEW;
    END IF;

    RAISE EXCEPTION
      'filesystem image provider contract transition is not an adoption rotation'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_contract_transition';
  END IF;

  IF NEW.operation_index_adoption_id IS DISTINCT FROM OLD.operation_index_adoption_id
    OR NEW.operation_index_adoption_xid IS DISTINCT FROM OLD.operation_index_adoption_xid
  THEN
    RAISE EXCEPTION
      'filesystem image provider adoption identity is immutable'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_adoption_immutable';
  END IF;

  IF OLD.operation_index_state_revision IS NULL THEN
    IF NEW.operation_index_state_revision IS NOT NULL THEN
      RAISE EXCEPTION
        'filesystem image provider operation index cannot be enabled without adoption'
        USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_index_activation';
    END IF;
    RETURN NEW;
  END IF;

  IF NEW.operation_index_state_revision IS NULL
    OR NOT (
      NEW.anchor_revision = OLD.anchor_revision + 1
      AND NEW.generation = OLD.generation
      AND NEW.state_revision = OLD.state_revision + 1
      AND NEW.base_head_checksum IS NOT DISTINCT FROM OLD.base_head_checksum
      AND NEW.checkpoint_state_revision = OLD.checkpoint_state_revision
      AND NEW.checkpoint_frame_count = OLD.checkpoint_frame_count
      AND NEW.checkpoint_checksum IS NOT DISTINCT FROM OLD.checkpoint_checksum
      AND NEW.checkpoint_bytes = OLD.checkpoint_bytes
      AND NEW.frame_count = OLD.frame_count + 1
      AND NEW.last_checksum IS NOT NULL
      AND NEW.ledger_bytes > OLD.ledger_bytes
      OR NEW.anchor_revision = OLD.anchor_revision + 1
        AND NEW.generation = OLD.generation + 1
        AND NEW.state_revision = OLD.state_revision
        AND OLD.frame_count > 0
        AND OLD.ledger_bytes > 0
        AND NEW.base_head_checksum IS NOT NULL
        AND NEW.checkpoint_state_revision = OLD.state_revision
        AND NEW.checkpoint_frame_count BETWEEN 2 AND 4294967295
        AND NEW.checkpoint_checksum IS NOT NULL
        AND NEW.checkpoint_bytes BETWEEN 1 AND 9007199254740991
        AND NEW.frame_count = 0
        AND NEW.last_checksum = NEW.checkpoint_checksum
        AND NEW.ledger_bytes = 0
    )
  THEN
    RAISE EXCEPTION
      'indexed filesystem image provider head update is not one append or rotation'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_incremental_progress';
  END IF;
  RETURN NEW;
END
$enforce_fs_image_head_adoption$;

CREATE TRIGGER fs_image_heads_adoption_guard
BEFORE INSERT OR UPDATE ON session_authority.filesystem_image_provider_heads
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_fs_image_head_adoption();

-- Claim the permanent lifecycle only after the head row has acquired its
-- unique-index slot. Head deletion uses the same head-then-lifecycle lock
-- order, avoiding a lifecycle/head lock inversion during concurrent teardown.
CREATE FUNCTION session_authority.claim_fs_image_anchor_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $claim_fs_image_anchor_lifecycle$
DECLARE
  lifecycle_retired_xid xid8;
BEGIN
  INSERT INTO session_authority.filesystem_image_provider_anchor_lifecycle
    (provider_id, anchor_id, retired_xid)
  VALUES (NEW.provider_id, NEW.anchor_id, NULL)
  ON CONFLICT (provider_id, anchor_id) DO UPDATE
    SET anchor_id = EXCLUDED.anchor_id
  RETURNING retired_xid INTO lifecycle_retired_xid;

  IF lifecycle_retired_xid IS NOT NULL
  THEN
    RAISE EXCEPTION
      'filesystem image provider retired anchor identity cannot be reused'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_head_retired';
  END IF;
  RETURN NEW;
END
$claim_fs_image_anchor_lifecycle$;

CREATE TRIGGER fs_image_heads_lifecycle_claim
AFTER INSERT ON session_authority.filesystem_image_provider_heads
FOR EACH ROW
EXECUTE FUNCTION session_authority.claim_fs_image_anchor_lifecycle();

CREATE FUNCTION session_authority.retire_fs_image_anchor()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $retire_fs_image_anchor$
BEGIN
  UPDATE session_authority.filesystem_image_provider_anchor_lifecycle
  SET retired_xid = pg_current_xact_id()
  WHERE provider_id = OLD.provider_id
    AND anchor_id = OLD.anchor_id
    AND retired_xid IS NULL;

  IF NOT FOUND
    AND NOT EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_anchor_lifecycle AS lifecycle
      WHERE lifecycle.provider_id = OLD.provider_id
        AND lifecycle.anchor_id = OLD.anchor_id
        AND lifecycle.retired_xid = pg_current_xact_id()
    )
  THEN
    RAISE EXCEPTION 'filesystem image provider anchor lifecycle is invalid'
      USING ERRCODE = '55000', CONSTRAINT = 'fs_image_anchor_lifecycle_invalid';
  END IF;
  RETURN OLD;
END
$retire_fs_image_anchor$;

CREATE TRIGGER fs_image_heads_retirement_guard
AFTER DELETE ON session_authority.filesystem_image_provider_heads
FOR EACH ROW
EXECUTE FUNCTION session_authority.retire_fs_image_anchor();

CREATE FUNCTION session_authority.reject_fs_image_head_truncate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $reject_fs_image_head_truncate$
BEGIN
  RAISE EXCEPTION 'filesystem image provider heads cannot be truncated'
    USING ERRCODE = '55000', CONSTRAINT = 'fs_image_heads_truncate_forbidden';
END
$reject_fs_image_head_truncate$;

CREATE TRIGGER fs_image_heads_truncate_guard
BEFORE TRUNCATE ON session_authority.filesystem_image_provider_heads
FOR EACH STATEMENT
EXECUTE FUNCTION session_authority.reject_fs_image_head_truncate();

-- This deferred check is the database-side completeness proof. A head token,
-- covering checkpoint, or copied manifest identifier cannot commit unless the
-- permanent index contains each event revision exactly once from 1 through N.
CREATE FUNCTION session_authority.assert_fs_image_adoption_rows(
  selected_provider_id character varying,
  selected_anchor_id character varying,
  selected_adoption_id character varying,
  selected_checkpoint_revision numeric,
  selected_state_revision numeric,
  selected_mode integer
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $assert_fs_image_adoption_rows$
DECLARE
  event_count numeric;
  first_revision numeric;
  last_revision numeric;
BEGIN
  SELECT
    coalesce(sum(1::numeric), 0),
    min(revision),
    max(revision)
  INTO event_count, first_revision, last_revision
  FROM (
    SELECT prepared_state_revision AS revision
    FROM session_authority.filesystem_image_provider_operations
    WHERE provider_id = selected_provider_id
      AND anchor_id = selected_anchor_id
    UNION ALL
    SELECT committed_state_revision AS revision
    FROM session_authority.filesystem_image_provider_operations
    WHERE provider_id = selected_provider_id
      AND anchor_id = selected_anchor_id
      AND state = 'committed'
  ) AS events;

  IF event_count <> selected_state_revision
    OR (
      selected_state_revision = 0
      AND (first_revision IS NOT NULL OR last_revision IS NOT NULL)
    )
    OR (
      selected_state_revision > 0
      AND (first_revision <> 1 OR last_revision <> selected_state_revision)
    )
  THEN
    RAISE EXCEPTION 'filesystem image provider adoption revisions are incomplete'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_revision_cover';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM (
      SELECT prepared_state_revision AS revision
      FROM session_authority.filesystem_image_provider_operations
      WHERE provider_id = selected_provider_id
        AND anchor_id = selected_anchor_id
      UNION ALL
      SELECT committed_state_revision AS revision
      FROM session_authority.filesystem_image_provider_operations
      WHERE provider_id = selected_provider_id
        AND anchor_id = selected_anchor_id
        AND state = 'committed'
    ) AS events
    GROUP BY revision
    HAVING sum(1::numeric) <> 1
  )
  THEN
    RAISE EXCEPTION 'filesystem image provider adoption revisions are duplicated'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_revision_unique';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM session_authority.filesystem_image_provider_operations AS operation
    WHERE operation.provider_id = selected_provider_id
      AND operation.anchor_id = selected_anchor_id
      AND (
        operation.adoption_id IS NOT NULL
          AND (
            operation.adoption_id <> selected_adoption_id
            OR operation.state = 'committed'
              AND operation.committed_checksum_provenance
                IS DISTINCT FROM 'unavailable-adopted-v2'
          )
        OR operation.committed_checksum_provenance = 'unavailable-adopted-v2'
          AND (
            operation.adoption_id IS DISTINCT FROM selected_adoption_id
            OR operation.committed_state_revision > selected_checkpoint_revision
          )
      )
  )
  THEN
    RAISE EXCEPTION 'filesystem image provider adoption row binding is invalid'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_row_binding';
  END IF;

  IF selected_mode = 1
    AND EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_operations AS operation
      WHERE operation.provider_id = selected_provider_id
        AND operation.anchor_id = selected_anchor_id
        AND operation.adoption_id IS DISTINCT FROM selected_adoption_id
    )
  THEN
    RAISE EXCEPTION 'legacy filesystem image provider rows are not one adoption'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_legacy_rows';
  ELSIF selected_mode = 0
    AND EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_operations AS operation
      WHERE operation.provider_id = selected_provider_id
        AND operation.anchor_id = selected_anchor_id
        AND (
          operation.adoption_id IS NOT NULL
          OR operation.state = 'committed'
            AND operation.committed_checksum_provenance
              IS DISTINCT FROM 'indexed-frame-v1'
        )
    )
  THEN
    RAISE EXCEPTION 'indexed filesystem image provider rows changed provenance'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_indexed_rows';
  ELSIF selected_mode = -1
    AND EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_operations AS operation
      WHERE operation.provider_id = selected_provider_id
        AND operation.anchor_id = selected_anchor_id
        AND operation.adoption_id IS NULL
    )
    AND EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_operations AS operation
      WHERE operation.provider_id = selected_provider_id
        AND operation.anchor_id = selected_anchor_id
        AND operation.adoption_id IS NOT NULL
    )
  THEN
    RAISE EXCEPTION 'filesystem image provider adoption row modes are mixed'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_row_mode';
  ELSIF selected_mode NOT IN (-1, 0, 1)
  THEN
    RAISE EXCEPTION 'filesystem image provider adoption validation mode is invalid'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_mode';
  END IF;
END
$assert_fs_image_adoption_rows$;

CREATE FUNCTION session_authority.validate_fs_image_adoption()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $validate_fs_image_adoption$
DECLARE
  stored_head session_authority.filesystem_image_provider_heads%ROWTYPE;
BEGIN
  IF OLD.operation_index_adoption_id IS NOT NULL
    OR NEW.operation_index_adoption_id IS NULL
    OR NEW.operation_index_adoption_xid IS DISTINCT FROM pg_current_xact_id()
  THEN
    RETURN NEW;
  END IF;

  SELECT * INTO stored_head
  FROM session_authority.filesystem_image_provider_heads
  WHERE provider_id = NEW.provider_id AND anchor_id = NEW.anchor_id;

  IF NOT FOUND THEN
    IF EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_operations
      WHERE provider_id = NEW.provider_id AND anchor_id = NEW.anchor_id
    )
      OR EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_heads AS head
        WHERE head.operation_index_adoption_id = NEW.operation_index_adoption_id
          AND head.operation_index_adoption_xid = NEW.operation_index_adoption_xid
      )
      OR NOT EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_anchor_lifecycle AS lifecycle
        WHERE lifecycle.provider_id = NEW.provider_id
          AND lifecycle.anchor_id = NEW.anchor_id
          AND lifecycle.retired_xid = pg_current_xact_id()
      )
    THEN
      RAISE EXCEPTION 'filesystem image provider teardown left adoption state'
        USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_teardown';
    END IF;
    RETURN NEW;
  END IF;

  IF stored_head.provider_id IS DISTINCT FROM NEW.provider_id
    OR stored_head.anchor_id IS DISTINCT FROM NEW.anchor_id
    OR stored_head.contract_version IS DISTINCT FROM NEW.contract_version
    OR stored_head.anchor_revision IS DISTINCT FROM NEW.anchor_revision
    OR stored_head.generation IS DISTINCT FROM NEW.generation
    OR stored_head.state_revision IS DISTINCT FROM NEW.state_revision
    OR stored_head.base_head_checksum IS DISTINCT FROM NEW.base_head_checksum
    OR stored_head.checkpoint_state_revision IS DISTINCT FROM NEW.checkpoint_state_revision
    OR stored_head.checkpoint_frame_count IS DISTINCT FROM NEW.checkpoint_frame_count
    OR stored_head.checkpoint_checksum IS DISTINCT FROM NEW.checkpoint_checksum
    OR stored_head.checkpoint_bytes IS DISTINCT FROM NEW.checkpoint_bytes
    OR stored_head.frame_count IS DISTINCT FROM NEW.frame_count
    OR stored_head.last_checksum IS DISTINCT FROM NEW.last_checksum
    OR stored_head.ledger_bytes IS DISTINCT FROM NEW.ledger_bytes
    OR stored_head.operation_index_state_revision IS DISTINCT FROM NEW.operation_index_state_revision
    OR stored_head.operation_index_adoption_id IS DISTINCT FROM NEW.operation_index_adoption_id
    OR stored_head.operation_index_adoption_xid IS DISTINCT FROM NEW.operation_index_adoption_xid
    OR stored_head.operation_index_progress_xid IS DISTINCT FROM NEW.operation_index_progress_xid
    OR stored_head.contract_version <> 3
    OR stored_head.operation_index_state_revision IS DISTINCT FROM stored_head.state_revision
    OR stored_head.checkpoint_state_revision IS DISTINCT FROM stored_head.state_revision
    OR stored_head.frame_count <> 0
    OR stored_head.ledger_bytes <> 0
  THEN
    RAISE EXCEPTION 'filesystem image provider adoption head is incomplete'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_adoption_head_complete';
  END IF;

  PERFORM session_authority.assert_fs_image_adoption_rows(
    NEW.provider_id,
    NEW.anchor_id,
    NEW.operation_index_adoption_id,
    NEW.checkpoint_state_revision,
    NEW.state_revision,
    CASE WHEN OLD.operation_index_state_revision IS NULL THEN 1 ELSE 0 END
  );
  RETURN NEW;
END
$validate_fs_image_adoption$;

CREATE CONSTRAINT TRIGGER fs_image_heads_adoption_complete
AFTER UPDATE ON session_authority.filesystem_image_provider_heads
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.validate_fs_image_adoption();

-- Indexed history is maintained inductively after migration/adoption: a new
-- stored head has exactly its first event, and every later head mutation is
-- one append with one newly claimed revision or one rotation
-- with no new revision. The final-row comparison also prevents an early
-- SET CONSTRAINTS check from authorizing a later second head mutation.
CREATE FUNCTION session_authority.validate_fs_image_head_progress()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $validate_fs_image_head_progress$
DECLARE
  stored_head session_authority.filesystem_image_provider_heads%ROWTYPE;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF OLD.contract_version = 2
      AND NEW.contract_version = 3
      AND OLD.operation_index_adoption_id IS NULL
      AND NEW.operation_index_adoption_id IS NOT NULL
      AND NEW.operation_index_adoption_xid = pg_current_xact_id()
    THEN
      RETURN NEW;
    END IF;
  END IF;

  IF NEW.operation_index_state_revision IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT * INTO stored_head
  FROM session_authority.filesystem_image_provider_heads
  WHERE provider_id = NEW.provider_id AND anchor_id = NEW.anchor_id;

  IF NOT FOUND THEN
    IF EXISTS (
      SELECT 1
      FROM session_authority.filesystem_image_provider_operations
      WHERE provider_id = NEW.provider_id AND anchor_id = NEW.anchor_id
    )
      OR EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_operation_events
        WHERE provider_id = NEW.provider_id AND anchor_id = NEW.anchor_id
      )
      OR NOT EXISTS (
        SELECT 1
        FROM session_authority.filesystem_image_provider_anchor_lifecycle AS lifecycle
        WHERE lifecycle.provider_id = NEW.provider_id
          AND lifecycle.anchor_id = NEW.anchor_id
          AND lifecycle.retired_xid = pg_current_xact_id()
      )
    THEN
      RAISE EXCEPTION 'filesystem image provider teardown left indexed state'
        USING ERRCODE = '23514', CONSTRAINT = 'fs_image_head_progress_teardown';
    END IF;
    RETURN NEW;
  END IF;

  IF stored_head.provider_id IS DISTINCT FROM NEW.provider_id
    OR stored_head.anchor_id IS DISTINCT FROM NEW.anchor_id
    OR stored_head.contract_version IS DISTINCT FROM NEW.contract_version
    OR stored_head.anchor_revision IS DISTINCT FROM NEW.anchor_revision
    OR stored_head.generation IS DISTINCT FROM NEW.generation
    OR stored_head.state_revision IS DISTINCT FROM NEW.state_revision
    OR stored_head.base_head_checksum IS DISTINCT FROM NEW.base_head_checksum
    OR stored_head.checkpoint_state_revision IS DISTINCT FROM NEW.checkpoint_state_revision
    OR stored_head.checkpoint_frame_count IS DISTINCT FROM NEW.checkpoint_frame_count
    OR stored_head.checkpoint_checksum IS DISTINCT FROM NEW.checkpoint_checksum
    OR stored_head.checkpoint_bytes IS DISTINCT FROM NEW.checkpoint_bytes
    OR stored_head.frame_count IS DISTINCT FROM NEW.frame_count
    OR stored_head.last_checksum IS DISTINCT FROM NEW.last_checksum
    OR stored_head.ledger_bytes IS DISTINCT FROM NEW.ledger_bytes
    OR stored_head.operation_index_state_revision IS DISTINCT FROM NEW.operation_index_state_revision
    OR stored_head.operation_index_adoption_id IS DISTINCT FROM NEW.operation_index_adoption_id
    OR stored_head.operation_index_adoption_xid IS DISTINCT FROM NEW.operation_index_adoption_xid
    OR stored_head.operation_index_progress_xid IS DISTINCT FROM NEW.operation_index_progress_xid
  THEN
    RAISE EXCEPTION 'filesystem image provider indexed head changed after validation'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_head_progress_final';
  END IF;

  IF TG_OP = 'UPDATE' THEN
    IF NEW.state_revision = OLD.state_revision THEN
      RETURN NEW;
    END IF;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM session_authority.filesystem_image_provider_operation_events AS event
    JOIN session_authority.filesystem_image_provider_operations AS operation
      ON operation.provider_id = event.provider_id
      AND operation.anchor_id = event.anchor_id
      AND operation.operation_id = event.operation_id
    WHERE event.provider_id = NEW.provider_id
      AND event.anchor_id = NEW.anchor_id
      AND event.event_revision = NEW.state_revision
      AND (
        event.phase = 'prepared'
          AND operation.prepared_state_revision = NEW.state_revision
          AND operation.prepared_checksum = NEW.last_checksum
        OR event.phase = 'committed'
          AND operation.state = 'committed'
          AND operation.committed_state_revision = NEW.state_revision
          AND operation.committed_checksum_provenance = 'indexed-frame-v1'
          AND operation.committed_checksum = NEW.last_checksum
      )
  )
  THEN
    RAISE EXCEPTION 'filesystem image provider append event is missing'
      USING ERRCODE = '23514', CONSTRAINT = 'fs_image_head_append_event';
  END IF;
  RETURN NEW;
END
$validate_fs_image_head_progress$;

CREATE CONSTRAINT TRIGGER fs_image_heads_progress_complete
AFTER INSERT OR UPDATE ON session_authority.filesystem_image_provider_heads
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.validate_fs_image_head_progress();
