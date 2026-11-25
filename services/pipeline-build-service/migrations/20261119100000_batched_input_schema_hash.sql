-- Store the opt-in precondition alongside the build's frozen input binding.
-- An empty default preserves existing and non-opted-in builds.
ALTER TABLE build_batched_configs
    ADD COLUMN IF NOT EXISTS expected_input_schema_hash TEXT NOT NULL DEFAULT '';
