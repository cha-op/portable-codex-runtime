-- Genesis is represented only by row absence so its CAS is an exact insert.
CREATE TABLE session_authority.filesystem_image_provider_heads (
  provider_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  anchor_id character varying(128) COLLATE pg_catalog."C" NOT NULL,
  contract_version integer NOT NULL,
  sequence integer NOT NULL,
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
    CHECK (contract_version = 1),
  CONSTRAINT filesystem_image_provider_heads_sequence_bounded
    CHECK (sequence BETWEEN 1 AND 65535),
  CONSTRAINT filesystem_image_provider_heads_ledger_bytes_bounded
    CHECK (ledger_bytes BETWEEN 1 AND 67108864),
  CONSTRAINT filesystem_image_provider_heads_checksum_format
    CHECK ((
      octet_length(last_checksum) = 64
      AND last_checksum !~ '[^0-9a-f]'
    ) IS TRUE)
);
