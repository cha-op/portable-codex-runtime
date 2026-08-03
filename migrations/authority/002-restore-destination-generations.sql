ALTER TABLE session_authority.checkpoint_catalogue
  ADD CONSTRAINT checkpoint_catalogue_checkpoint_session_unique
  UNIQUE (checkpoint_id, session_id);

CREATE TABLE session_authority.restore_destination_generations (
  generation_id character varying(128) PRIMARY KEY,
  operation_id character varying(128) NOT NULL UNIQUE,
  session_id uuid NOT NULL,
  checkpoint_id character varying(128) NOT NULL,
  state character varying(32) NOT NULL,
  binding jsonb NOT NULL,
  document jsonb,
  claimed_at timestamp with time zone NOT NULL,
  committed_at timestamp with time zone,
  CONSTRAINT restore_destination_generations_generation_id_length
    CHECK (octet_length(generation_id) BETWEEN 1 AND 128),
  CONSTRAINT restore_destination_generations_operation_id_length
    CHECK (octet_length(operation_id) BETWEEN 1 AND 128),
  CONSTRAINT restore_destination_generations_checkpoint_id_length
    CHECK (octet_length(checkpoint_id) BETWEEN 1 AND 128),
  CONSTRAINT restore_destination_generations_state_allowed
    CHECK (state IN ('authorized', 'committed')),
  CONSTRAINT restore_destination_generations_binding_object
    CHECK (jsonb_typeof(binding) = 'object'),
  CONSTRAINT restore_destination_generations_document_object
    CHECK (document IS NULL OR jsonb_typeof(document) = 'object'),
  CONSTRAINT restore_destination_generations_state_payload_pair
    CHECK (
      (
        state = 'authorized'
        AND document IS NULL
        AND committed_at IS NULL
      )
      OR
      (
        state = 'committed'
        AND document IS NOT NULL
        AND committed_at IS NOT NULL
      )
    ),
  CONSTRAINT restore_destination_generations_committed_at_order
    CHECK (committed_at IS NULL OR committed_at >= claimed_at),
  CONSTRAINT restore_destination_generations_operation_session_fk
    FOREIGN KEY (operation_id, session_id)
    REFERENCES session_authority.operation_claims(operation_id, session_id),
  CONSTRAINT restore_destination_generations_checkpoint_session_fk
    FOREIGN KEY (checkpoint_id, session_id)
    REFERENCES session_authority.checkpoint_catalogue(
      checkpoint_id,
      session_id
    )
);
