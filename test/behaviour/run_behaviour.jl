using Gherkin
using TypeDBClient3
using Test

# ─── Load step definitions ─────────────────────────────────────────────────────
# Steps must be loaded in this order: context first (defines CTX, helpers, hook)
include(joinpath(@__DIR__, "steps", "context.jl"))
include(joinpath(@__DIR__, "steps", "connection_steps.jl"))
include(joinpath(@__DIR__, "steps", "query_steps.jl"))

# ─── Run all feature files ─────────────────────────────────────────────────────
const FEATURES_DIR = joinpath(@__DIR__, "features")

for (root, dirs, files) in walkdir(FEATURES_DIR)
    sort!(files)
    for file in files
        endswith(file, ".feature") || continue
        feature_path = joinpath(root, file)
        @info "Running feature: $feature_path"
        feature = Gherkin.parse_feature(feature_path)
        Gherkin.run_feature(feature, Gherkin.GLOBAL_REGISTRY)
    end
end
