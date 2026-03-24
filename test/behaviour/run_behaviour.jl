using Gherkin
using TypeDBClient3
using Test

# ─── Load step definitions ─────────────────────────────────────────────────────
# Steps must be loaded in this order: context first (defines CTX, helpers, hook)
include(joinpath(@__DIR__, "steps", "context.jl"))
include(joinpath(@__DIR__, "steps", "connection_steps.jl"))
include(joinpath(@__DIR__, "steps", "query_steps.jl"))

# ─── Tag configuration ─────────────────────────────────────────────────────────
# Scenarios tagged with any of these are skipped (driver-specific exclusions from
# the shared typedb/typedb-behaviour repository).
const BEHAVIOUR_EXCLUDE_TAGS = [
    "ignore-typedb-driver-java",
    "ignore-typedb-http",
]

# ─── Run all feature files ─────────────────────────────────────────────────────
const FEATURES_DIR = joinpath(@__DIR__, "features")

# Optional filter: set BEHAVIOUR_FEATURE_FILES to a comma-separated list of
# relative paths (e.g. "driver/query.feature,driver/connection.feature") to
# restrict which feature files are executed.
const _FEATURE_FILTER = let v = get(ENV, "BEHAVIOUR_FEATURE_FILES", "")
    isempty(v) ? nothing : Set(split(v, ",") .|> strip)
end

for (root, dirs, files) in walkdir(FEATURES_DIR)
    sort!(files)
    for file in files
        endswith(file, ".feature") || continue
        feature_path = joinpath(root, file)
        # Apply optional filter (match against relative path from FEATURES_DIR)
        if _FEATURE_FILTER !== nothing
            rel = relpath(feature_path, FEATURES_DIR)
            rel in _FEATURE_FILTER || continue
        end
        @info "Running feature: $file"
        feature = Gherkin.parse_feature(feature_path)
        try
            Gherkin.run_feature(feature, Gherkin.GLOBAL_REGISTRY;
                                exclude_tags = BEHAVIOUR_EXCLUDE_TAGS)
        catch e
            e isa Test.TestSetException || rethrow()
            # TestSetException means some @tests failed — already reported above.
            # Continue running remaining feature files so we get full coverage.
        end
    end
end
