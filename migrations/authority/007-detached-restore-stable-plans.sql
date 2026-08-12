-- Runtime writers lock the session row before touching operation relations.
-- Preserve that order while extending the permanent operation-ID namespace.
LOCK TABLE session_authority.sessions IN EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE;

LOCK TABLE session_authority.operation_id_registry IN ACCESS EXCLUSIVE MODE;

ALTER TABLE session_authority.operation_id_registry
  DROP CONSTRAINT operation_id_registry_claim_type_allowed;

ALTER TABLE session_authority.operation_id_registry
  ADD CONSTRAINT operation_id_registry_claim_type_allowed
  CHECK (
    claim_type IN (
      'direct-operation',
      'restore-launch-intent-v2',
      'restore-activation-launch-intent-v1',
      'writer-stop-capture-intent-v3',
      'detached-restore-stable-plan-v1'
    )
  );

ALTER TABLE session_authority.operation_id_registry
  DROP CONSTRAINT operation_id_registry_claim_shape;

ALTER TABLE session_authority.operation_id_registry
  ADD CONSTRAINT operation_id_registry_claim_shape
  CHECK ((
    (
      claim_type = 'direct-operation'
      AND claimant_operation_id IS NULL
      AND binding IS NULL
      AND materialized_at IS NOT NULL
      AND materialized_at = claimed_at
    )
    OR
    (
      claim_type IN (
        'restore-launch-intent-v2',
        'restore-activation-launch-intent-v1',
        'writer-stop-capture-intent-v3'
      )
      AND claimant_operation_id IS NOT NULL
      AND claimant_operation_id <> operation_id
      AND binding IS NOT NULL
    )
    OR
    (
      claim_type = 'detached-restore-stable-plan-v1'
      AND claimant_operation_id IS NULL
      AND binding IS NOT NULL
      AND pg_catalog.jsonb_object_length(binding) = 4
      AND binding ? 'bindingSha256'
      AND binding ? 'contractVersion'
      AND binding ? 'planSha256'
      AND binding ? 'request'
      AND binding -> 'contractVersion' = '1'::pg_catalog.jsonb
      AND pg_catalog.jsonb_typeof(binding -> 'bindingSha256') = 'string'
      AND binding ->> 'bindingSha256' ~ '^[0-9a-f]{64}$'
      AND pg_catalog.jsonb_typeof(binding -> 'planSha256') = 'string'
      AND binding ->> 'planSha256' ~ '^[0-9a-f]{64}$'
      AND pg_catalog.jsonb_typeof(binding -> 'request') = 'object'
      AND (
        materialized_at IS NULL
        OR materialized_at >= claimed_at
      )
    )
  ) IS TRUE);

CREATE TABLE session_authority.detached_restore_stable_plans (
  operation_id character varying(128) PRIMARY KEY,
  session_id uuid NOT NULL,
  backend_id character varying(128) NOT NULL,
  storage_id character varying(128) NOT NULL,
  plan_contract_version integer NOT NULL,
  admission jsonb NOT NULL,
  plan_input jsonb NOT NULL,
  plan_sha256 character(64) NOT NULL,
  binding_sha256 character(64) NOT NULL,
  provisioned_at timestamp with time zone NOT NULL,
  CONSTRAINT detached_restore_stable_plans_operation_session_unique
    UNIQUE (operation_id, session_id),
  CONSTRAINT detached_restore_stable_plans_operation_id_length
    CHECK (octet_length(operation_id) BETWEEN 1 AND 128),
  CONSTRAINT detached_restore_stable_plans_backend_id_length
    CHECK (octet_length(backend_id) BETWEEN 1 AND 128),
  CONSTRAINT detached_restore_stable_plans_storage_id_length
    CHECK (octet_length(storage_id) BETWEEN 1 AND 128),
  CONSTRAINT detached_restore_stable_plans_contract_version_supported
    CHECK (plan_contract_version = 1),
  CONSTRAINT detached_restore_stable_plans_admission_object
    CHECK ((
      pg_catalog.jsonb_typeof(admission) = 'object'
      AND pg_catalog.jsonb_object_length(admission) = 2
      AND admission ? 'checkpoint'
      AND admission ? 'request'
      AND pg_catalog.jsonb_typeof(admission -> 'checkpoint') = 'object'
      AND pg_catalog.jsonb_typeof(admission -> 'request') = 'object'
    ) IS TRUE),
  CONSTRAINT detached_restore_stable_plans_plan_input_object
    CHECK ((
      pg_catalog.jsonb_typeof(plan_input) = 'object'
      AND pg_catalog.jsonb_object_length(plan_input) = 9
      AND plan_input ? 'captureCreatedAt'
      AND plan_input ? 'destinationDirectory'
      AND plan_input ? 'destinationOwnedRoot'
      AND plan_input ? 'detachMode'
      AND plan_input ? 'holderId'
      AND plan_input ? 'imagePlanId'
      AND plan_input ? 'leaseDurationMilliseconds'
      AND plan_input ? 'sourceArtifactDirectory'
      AND plan_input ? 'sourceArtifactOwnedRoot'
      AND pg_catalog.jsonb_typeof(
        plan_input -> 'captureCreatedAt'
      ) = 'string'
      AND pg_catalog.jsonb_typeof(
        plan_input -> 'destinationDirectory'
      ) = 'string'
      AND pg_catalog.jsonb_typeof(
        plan_input -> 'destinationOwnedRoot'
      ) = 'string'
      AND pg_catalog.jsonb_typeof(plan_input -> 'detachMode') = 'string'
      AND pg_catalog.jsonb_typeof(plan_input -> 'holderId') = 'string'
      AND pg_catalog.jsonb_typeof(plan_input -> 'imagePlanId') = 'string'
      AND pg_catalog.jsonb_typeof(
        plan_input -> 'leaseDurationMilliseconds'
      ) = 'number'
      AND pg_catalog.jsonb_typeof(
        plan_input -> 'sourceArtifactDirectory'
      ) = 'string'
      AND pg_catalog.jsonb_typeof(
        plan_input -> 'sourceArtifactOwnedRoot'
      ) = 'string'
    ) IS TRUE),
  CONSTRAINT detached_restore_stable_plans_plan_sha256_format
    CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT detached_restore_stable_plans_binding_sha256_format
    CHECK (binding_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT detached_restore_stable_plans_request_identity
    CHECK ((
      pg_catalog.jsonb_typeof(
        admission #> '{request,operationId}'
      ) = 'string'
      AND admission #>> '{request,operationId}' = operation_id
      AND pg_catalog.jsonb_typeof(
        admission #> '{request,sessionId}'
      ) = 'string'
      AND admission #>> '{request,sessionId}' =
        session_id::pg_catalog.text
      AND pg_catalog.jsonb_typeof(
        admission #> '{request,backendId}'
      ) = 'string'
      AND admission #>> '{request,backendId}' = backend_id
      AND pg_catalog.jsonb_typeof(
        admission #> '{request,storageId}'
      ) = 'string'
      AND admission #>> '{request,storageId}' = storage_id
    ) IS TRUE),
  CONSTRAINT detached_restore_stable_plans_request_shape
    CHECK ((
      pg_catalog.jsonb_typeof(
        admission #> '{request,contractVersion}'
      ) = 'number'
      AND admission #> '{request,contractVersion}' = '1'::pg_catalog.jsonb
      AND pg_catalog.jsonb_typeof(
        admission #> '{request,operation}'
      ) = 'string'
      AND admission #>> '{request,operation}' = 'restore'
      AND pg_catalog.jsonb_typeof(
        admission #> '{request,target}'
      ) = 'object'
      AND pg_catalog.jsonb_typeof(
        admission #> '{request,target,kind}'
      ) = 'string'
      AND admission #>> '{request,target,kind}' = 'checkpoint'
    ) IS TRUE),
  CONSTRAINT detached_restore_stable_plans_operation_registry_fk
    FOREIGN KEY (operation_id, session_id)
    REFERENCES session_authority.operation_id_registry(
      operation_id,
      session_id
    )
);

CREATE INDEX detached_restore_stable_plans_session_operation_idx
  ON session_authority.detached_restore_stable_plans (
    session_id,
    operation_id
  );

CREATE FUNCTION session_authority.enforce_detached_restore_stable_plan_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_detached_restore_stable_plan_claim$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM session_authority.operation_id_registry AS registry
    WHERE registry.operation_id = NEW.operation_id
      AND registry.session_id = NEW.session_id
      AND registry.claim_type = 'detached-restore-stable-plan-v1'
      AND registry.claimant_operation_id IS NULL
      AND registry.binding = pg_catalog.jsonb_build_object(
        'bindingSha256', NEW.binding_sha256,
        'contractVersion', NEW.plan_contract_version,
        'planSha256', NEW.plan_sha256,
        'request', NEW.admission #> '{request}'
      )
      AND registry.claimed_at = NEW.provisioned_at
      AND registry.materialized_at IS NULL
  )
  THEN
    RAISE EXCEPTION
      'detached restore stable plan requires one exact operation ID claim'
      USING
        ERRCODE = '23514',
        CONSTRAINT = 'detached_restore_stable_plans_operation_id_claim';
  END IF;
  RETURN NEW;
END
$enforce_detached_restore_stable_plan_claim$;

CREATE TRIGGER detached_restore_stable_plans_enforce_operation_id_claim
BEFORE INSERT ON session_authority.detached_restore_stable_plans
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_detached_restore_stable_plan_claim();

CREATE FUNCTION session_authority.enforce_detached_restore_stable_plan_claim_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_detached_restore_stable_plan_claim_update$
BEGIN
  IF OLD.claim_type = 'detached-restore-stable-plan-v1'
    OR NEW.claim_type = 'detached-restore-stable-plan-v1'
  THEN
    IF NOT (
      OLD.claim_type = 'detached-restore-stable-plan-v1'
      AND NEW.claim_type = 'detached-restore-stable-plan-v1'
      AND NEW.operation_id IS NOT DISTINCT FROM OLD.operation_id
      AND NEW.session_id IS NOT DISTINCT FROM OLD.session_id
      AND NEW.claimant_operation_id IS NOT DISTINCT FROM OLD.claimant_operation_id
      AND NEW.binding IS NOT DISTINCT FROM OLD.binding
      AND NEW.claimed_at IS NOT DISTINCT FROM OLD.claimed_at
      AND OLD.materialized_at IS NULL
      AND NEW.materialized_at IS NOT NULL
    )
    THEN
      RAISE EXCEPTION
        'detached restore stable-plan claims are immutable after reservation'
        USING
          ERRCODE = '55000',
          CONSTRAINT = 'operation_id_registry_detached_restore_stable_plan_immutable';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_detached_restore_stable_plan_claim_update$;

CREATE TRIGGER operation_id_registry_stable_plan_update_guard
BEFORE UPDATE ON session_authority.operation_id_registry
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_detached_restore_stable_plan_claim_update();

-- The authority materializes the permanent claim before inserting the
-- operation in the same transaction. Reject a raw materialization update at
-- commit unless that exact operation now exists and retains the stable
-- admission.
CREATE FUNCTION session_authority.enforce_detached_restore_stable_plan_claim_materialization()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_detached_restore_stable_plan_claim_materialization$
BEGIN
  IF OLD.claim_type = 'detached-restore-stable-plan-v1'
    AND NEW.claim_type = 'detached-restore-stable-plan-v1'
    AND OLD.materialized_at IS NULL
    AND NEW.materialized_at IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM session_authority.operation_claims AS operation_claim
      JOIN session_authority.detached_restore_stable_plans AS stable
        ON stable.operation_id = operation_claim.operation_id
       AND stable.session_id = operation_claim.session_id
      WHERE operation_claim.operation_id = NEW.operation_id
        AND operation_claim.session_id = NEW.session_id
        AND operation_claim.created_at = NEW.materialized_at
        AND operation_claim.kind = 'restore-destination-generation-v1'
        AND operation_claim.request #> '{payload,contractVersion}' =
          '1'::pg_catalog.jsonb
        AND operation_claim.request #> '{payload,admission}' = stable.admission
    )
  THEN
    RAISE EXCEPTION
      'detached restore stable-plan claim materialization requires its exact operation'
      USING
        ERRCODE = '23514',
        CONSTRAINT = 'detached_restore_stable_plan_claim_materialization';
  END IF;
  RETURN NEW;
END
$enforce_detached_restore_stable_plan_claim_materialization$;

CREATE CONSTRAINT TRIGGER operation_id_registry_stable_plan_materialization_guard
AFTER UPDATE ON session_authority.operation_id_registry
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_detached_restore_stable_plan_claim_materialization();

CREATE FUNCTION session_authority.reject_detached_restore_stable_plan_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $reject_detached_restore_stable_plan_update$
BEGIN
  RAISE EXCEPTION
    'detached restore stable plans are immutable'
    USING
      ERRCODE = '55000',
      CONSTRAINT = 'detached_restore_stable_plans_immutable';
END
$reject_detached_restore_stable_plan_update$;

CREATE TRIGGER detached_restore_stable_plans_reject_update
BEFORE UPDATE ON session_authority.detached_restore_stable_plans
FOR EACH ROW
EXECUTE FUNCTION session_authority.reject_detached_restore_stable_plan_update();

-- Defer deletion validation so a complete FK-ordered session teardown may
-- remove the stable row before removing its permanent operation-ID claim.
CREATE FUNCTION session_authority.enforce_detached_restore_stable_plan_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_detached_restore_stable_plan_delete$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_id_registry AS registry
    WHERE registry.operation_id = OLD.operation_id
  )
  THEN
    RAISE EXCEPTION
      'detached restore stable plan deletion requires complete operation ID teardown'
      USING
        ERRCODE = '23503',
        CONSTRAINT = 'detached_restore_stable_plans_delete_requires_claim_teardown';
  END IF;
  RETURN OLD;
END
$enforce_detached_restore_stable_plan_delete$;

CREATE CONSTRAINT TRIGGER detached_restore_stable_plans_enforce_delete_teardown
AFTER DELETE ON session_authority.detached_restore_stable_plans
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_detached_restore_stable_plan_delete();

-- Defer operation deletion validation for the same FK-ordered teardown. A
-- materialized stable claim is permanent evidence that its exact operation
-- must remain until the plan and claim are removed in the same transaction.
CREATE FUNCTION session_authority.enforce_detached_restore_stable_plan_operation_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_detached_restore_stable_plan_operation_delete$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_id_registry AS registry
    WHERE registry.operation_id = OLD.operation_id
      AND registry.session_id = OLD.session_id
      AND registry.claim_type = 'detached-restore-stable-plan-v1'
  )
  THEN
    RAISE EXCEPTION
      'detached restore stable-plan operation deletion requires complete teardown'
      USING
        ERRCODE = '23503',
        CONSTRAINT = 'operation_claims_stable_plan_delete_requires_teardown';
  END IF;
  RETURN OLD;
END
$enforce_detached_restore_stable_plan_operation_delete$;

CREATE CONSTRAINT TRIGGER operation_claims_stable_plan_delete_teardown
AFTER DELETE ON session_authority.operation_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_detached_restore_stable_plan_operation_delete();

CREATE FUNCTION session_authority.enforce_detached_restore_stable_plan_materialization()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $enforce_detached_restore_stable_plan_materialization$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM session_authority.operation_id_registry AS registry
    WHERE registry.operation_id = NEW.operation_id
      AND registry.session_id = NEW.session_id
      AND registry.claim_type = 'detached-restore-stable-plan-v1'
  )
  THEN
    IF NOT EXISTS (
      SELECT 1
      FROM session_authority.operation_id_registry AS registry
      JOIN session_authority.detached_restore_stable_plans AS stable
        ON stable.operation_id = registry.operation_id
       AND stable.session_id = registry.session_id
      WHERE registry.operation_id = NEW.operation_id
        AND registry.session_id = NEW.session_id
        AND registry.claim_type = 'detached-restore-stable-plan-v1'
        AND registry.claimant_operation_id IS NULL
        AND registry.binding = pg_catalog.jsonb_build_object(
          'bindingSha256', stable.binding_sha256,
          'contractVersion', stable.plan_contract_version,
          'planSha256', stable.plan_sha256,
          'request', stable.admission #> '{request}'
        )
        AND registry.claimed_at = stable.provisioned_at
        AND registry.materialized_at = NEW.created_at
        AND NEW.kind = 'restore-destination-generation-v1'
        AND NEW.request #> '{payload,contractVersion}' =
          '1'::pg_catalog.jsonb
        AND NEW.request #> '{payload,admission}' = stable.admission
    )
    THEN
      RAISE EXCEPTION
        'detached restore stable plan materialization does not match its durable binding'
        USING
          ERRCODE = '23514',
          CONSTRAINT = 'operation_claims_detached_restore_stable_plan_materialization';
    END IF;
  END IF;
  RETURN NEW;
END
$enforce_detached_restore_stable_plan_materialization$;

CREATE TRIGGER operation_claims_enforce_detached_restore_plan_materialization
BEFORE INSERT OR UPDATE ON session_authority.operation_claims
FOR EACH ROW
EXECUTE FUNCTION session_authority.enforce_detached_restore_stable_plan_materialization();
