using Gherkin
using TypeDBClient3
using Test

const TEST_ADDRESS = get(ENV, "TYPEDB_TEST_ADDRESS", "localhost:1729")

# ─── Global behaviour test state ──────────────────────────────────────────────

mutable struct BehaviourContext
    driver::Union{TypeDBDriver, Nothing}
    transaction::Union{Transaction, Nothing}
    transactions::Vector{Transaction}       # "many transactions" scenarios
    transactions_parallel::Vector{Transaction}  # parallel transactions
    background_tasks::Vector{Task}          # "in background" steps
    query_answer::Union{QueryAnswer, Nothing}
    query_answer_rows::Union{Vector{ConceptRow}, Nothing}  # materialised rows
    query_answer_docs::Union{Vector{String}, Nothing}      # materialised documents
    # Concurrent-read tracking: rows collected by "concurrently get answers of typeql
    # read query N times" and a cursor that advances as "concurrently process N rows"
    # steps consume them.  Decoupled from query_answer_rows so that interleaved
    # schema/write queries do not clobber the concurrent-read state.
    concurrent_rows::Union{Vector{ConceptRow}, Nothing}
    concurrent_cursor::Int
    tx_options_timeout_ms::Union{Int64, Nothing}
    tx_options_schema_lock_ms::Union{Int64, Nothing}
    query_options_include_instance_types::Union{Bool, Nothing}
    query_options_prefetch_size::Union{Int64, Nothing}
    query_options_include_query_structure::Union{Bool, Nothing}
end

const CTX = BehaviourContext(
    nothing, nothing,
    Transaction[], Transaction[], Task[],
    nothing, nothing, nothing,
    nothing, 0,
    nothing, nothing,
    nothing, nothing, nothing
)

function reset_context!(::Gherkin.ScenarioContext)
    # Close single transaction
    if CTX.transaction !== nothing
        try
            if isopen(CTX.transaction)
                close(CTX.transaction)
            end
        catch
        end
    end
    # Close multi-transactions
    for tx in CTX.transactions
        try isopen(tx) && close(tx) catch end
    end
    for tx in CTX.transactions_parallel
        try isopen(tx) && close(tx) catch end
    end
    # Wait for background tasks
    for t in CTX.background_tasks
        try wait(t) catch end
    end
    # Close driver
    if CTX.driver !== nothing
        try
            if isopen(CTX.driver)
                close(CTX.driver)
            end
        catch
        end
    end

    CTX.driver = nothing
    CTX.transaction = nothing
    empty!(CTX.transactions)
    empty!(CTX.transactions_parallel)
    empty!(CTX.background_tasks)
    CTX.query_answer = nothing
    CTX.query_answer_rows = nothing
    CTX.query_answer_docs = nothing
    CTX.concurrent_rows = nothing
    CTX.concurrent_cursor = 0
    CTX.tx_options_timeout_ms = nothing
    CTX.tx_options_schema_lock_ms = nothing
    CTX.query_options_include_instance_types = nothing
    CTX.query_options_prefetch_size = nothing
    CTX.query_options_include_query_structure = nothing
    nothing
end

@before reset_context!

# ─── Helpers ──────────────────────────────────────────────────────────────────

"""Return true if `s` can be parsed as an Int."""
_is_true(s) = strip(s) == "true"

function _tx_type_from_name(name::AbstractString)::Int32
    n = strip(name)
    if n == "read"
        return TransactionType.READ
    elseif n == "write"
        return TransactionType.WRITE
    elseif n == "schema"
        return TransactionType.SCHEMA
    else
        error("Unknown transaction type: $name")
    end
end

function _open_tx(db_name::AbstractString, tx_type::Int32)::Transaction
    open_transaction(CTX.driver, db_name, tx_type;
        timeout_ms     = CTX.tx_options_timeout_ms,
        schema_lock_ms = CTX.tx_options_schema_lock_ms)
end

# ─── Schema comparison ─────────────────────────────────────────────────────────
# TypeDB CE 3.8.1 changed schema output format: attributes-first ordering,
# expanded multi-line form with trailing commas, double newline after "define".
# We normalize both sides before comparing so the test is format-independent.

function _normalize_typeql_def(s::AbstractString)
    s = strip(s)
    # Collapse comma+whitespace (TypeDB 3.8.1 uses "entity foo,\n  sub bar")
    s = replace(s, r",\s+" => " ")
    # Collapse all remaining whitespace runs to a single space
    s = replace(s, r"\s+" => " ")
    return strip(s)
end

"""
    schema_defs_match(actual, expected)

Return true when every definition in `expected` (after semantic normalization)
is also present in `actual`.  Ordering and whitespace differences are ignored.
The comparison is a subset check so TypeDB may return extra built-in definitions.
"""
function schema_defs_match(actual::AbstractString, expected::AbstractString)
    function defs_set(s)
        # Strip leading "define" keyword (with surrounding whitespace/newlines)
        s = replace(strip(s), r"^\s*define\s*" => "")
        # Split on ";" — works for type defs and for fun bodies because all
        # whitespace is collapsed before the split, so semicolons inside
        # indented function bodies also become top-level separators.
        parts = split(s, ";")
        return Set(_normalize_typeql_def.(filter(p -> !isempty(strip(p)), parts)))
    end
    expected_set = defs_set(expected)
    actual_set   = defs_set(actual)
    return expected_set ⊆ actual_set
end

"""Run `f()`, expect it to throw.  Fails the test if no exception is raised."""
function expect_throws(f)
    threw = false
    try
        f()
    catch
        threw = true
    end
    @test threw
end

"""Run `f()`, expect it to throw with a message containing `msg_fragment`."""
function expect_throws_msg(f, msg_fragment)
    threw = false
    msg_ok = false
    try
        f()
    catch e
        threw = true
        errmsg = sprint(showerror, e)
        msg_ok = occursin(msg_fragment, errmsg)
        if !msg_ok
            @warn "Expected error message to contain: $(repr(msg_fragment))\nActual: $(errmsg)"
        end
    end
    @test threw
    @test msg_ok
end

"""Materialise all rows from the current query_answer into query_answer_rows."""
function _materialise_rows()
    CTX.query_answer_rows === nothing || return  # already done
    CTX.query_answer === nothing && error("No query_answer to materialise")
    if is_row_stream(CTX.query_answer)
        CTX.query_answer_rows = collect(rows(CTX.query_answer))
    else
        CTX.query_answer_rows = ConceptRow[]
    end
end

"""Execute a typeql query on the current transaction, applying stored query options."""
function _run_query(typeql::AbstractString)::QueryAnswer
    query(CTX.transaction, typeql;
        include_instance_types  = CTX.query_options_include_instance_types,
        prefetch_size           = CTX.query_options_prefetch_size,
        include_query_structure = CTX.query_options_include_query_structure)
end
