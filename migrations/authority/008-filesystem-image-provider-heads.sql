-- Canonical genesis is represented only by row absence, so its CAS is an
-- exact insert. Every stored row is either a non-empty active generation or a
-- completed pure rotation whose active generation is empty.
CREATE TABLE session_authority.filesystem_image_provider_heads (
  provider_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  anchor_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  contract_version integer NOT NULL,
  anchor_revision numeric(20, 0) NOT NULL,
  generation numeric(20, 0) NOT NULL,
  state_revision numeric(20, 0) NOT NULL,
  base_head_checksum character(64) COLLATE pg_catalog."C",
  checkpoint_state_revision numeric(20, 0) NOT NULL,
  checkpoint_frame_count bigint NOT NULL,
  checkpoint_checksum character(64) COLLATE pg_catalog."C",
  checkpoint_bytes bigint NOT NULL,
  frame_count integer NOT NULL,
  last_checksum character(64) COLLATE pg_catalog."C" NOT NULL,
  ledger_bytes bigint NOT NULL,
  PRIMARY KEY (provider_id, anchor_id),
  CONSTRAINT filesystem_image_provider_heads_provider_id_format
    CHECK ((
      octet_length(provider_id) BETWEEN 1 AND 128
      AND provider_id ~ '^[A-Za-z0-9]'
      AND provider_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_heads_anchor_id_format
    CHECK ((
      octet_length(anchor_id) BETWEEN 1 AND 128
      AND anchor_id ~ '^[A-Za-z0-9]'
      AND anchor_id !~ '[^A-Za-z0-9._:-]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_heads_contract_version_supported
    CHECK (contract_version = 2),
  CONSTRAINT filesystem_image_provider_heads_uint64_fields_bounded
    CHECK (
      anchor_revision BETWEEN 1 AND 18446744073709551615
      AND generation BETWEEN 0 AND 18446744073709551615
      AND state_revision BETWEEN 0 AND 18446744073709551615
      AND checkpoint_state_revision BETWEEN 0 AND 18446744073709551615
    ),
  CONSTRAINT filesystem_image_provider_heads_revision_topology
    CHECK (
      anchor_revision = state_revision + generation
      AND state_revision = checkpoint_state_revision + frame_count
    ),
  CONSTRAINT filesystem_image_provider_heads_checkpoint_frame_count_bounded
    CHECK (checkpoint_frame_count BETWEEN 0 AND 4294967295),
  CONSTRAINT filesystem_image_provider_heads_checkpoint_bytes_bounded
    CHECK (checkpoint_bytes BETWEEN 0 AND 9007199254740991),
  CONSTRAINT filesystem_image_provider_heads_frame_count_bounded
    CHECK (frame_count BETWEEN 0 AND 65535),
  CONSTRAINT filesystem_image_provider_heads_ledger_bytes_bounded
    CHECK (ledger_bytes BETWEEN 0 AND 67108864),
  CONSTRAINT filesystem_image_provider_heads_base_checksum_format
    CHECK ((
      base_head_checksum IS NULL
      OR (
        octet_length(base_head_checksum) = 64
        AND base_head_checksum !~ '[^0-9a-f]'
      )
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_heads_checkpoint_checksum_format
    CHECK ((
      checkpoint_checksum IS NULL
      OR (
        octet_length(checkpoint_checksum) = 64
        AND checkpoint_checksum !~ '[^0-9a-f]'
      )
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_heads_last_checksum_format
    CHECK ((
      octet_length(last_checksum) = 64
      AND last_checksum !~ '[^0-9a-f]'
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_heads_checkpoint_shape
    CHECK ((
      (
        generation = 0
        AND base_head_checksum IS NULL
        AND checkpoint_state_revision = 0
        AND checkpoint_frame_count = 0
        AND checkpoint_checksum IS NULL
        AND checkpoint_bytes = 0
      )
      OR (
        generation > 0
        AND base_head_checksum IS NOT NULL
        AND checkpoint_checksum IS NOT NULL
        AND checkpoint_frame_count BETWEEN 2 AND 4294967295
        AND checkpoint_bytes BETWEEN 1 AND 9007199254740991
      )
    ) IS TRUE),
  CONSTRAINT filesystem_image_provider_heads_active_shape
    CHECK ((
      (
        frame_count = 0
        AND generation > 0
        AND ledger_bytes = 0
        AND last_checksum = checkpoint_checksum
      )
      OR (
        frame_count BETWEEN 1 AND 65535
        AND ledger_bytes BETWEEN 1 AND 67108864
      )
    ) IS TRUE)
);
