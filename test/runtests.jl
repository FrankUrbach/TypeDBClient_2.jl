using Test
using TypeDBClient3

# ─── Unit tests (always run – no server required) ─────────────────────────────
@testset "TypeDBClient3 Unit Tests" begin
    include("unit/test_error.jl")
    include("unit/test_strings.jl")
    include("unit/test_concept.jl")
end

# ─── Integration tests (require a running TypeDB server) ─────────────────────
const TEST_ADDRESS = get(ENV, "TYPEDB_TEST_ADDRESS", "")

if !isempty(TEST_ADDRESS)
    @info "Running integration tests against $TEST_ADDRESS"
    @testset "TypeDBClient3 Integration Tests" begin
        include("integration/test_driver.jl")
        include("integration/test_databases.jl")
        include("integration/test_transaction.jl")
        include("integration/test_query.jl")
        include("integration/test_concept_types.jl")
    end

    @testset "TypeDBClient3 Behaviour Tests" begin
        include("behaviour/run_behaviour.jl")
    end
else
    @info "Skipping integration tests (set TYPEDB_TEST_ADDRESS=host:port to enable)"
end;
