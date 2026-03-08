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
    tx_options_timeout_ms::Union{Int64, Nothing}
    tx_options_schema_lock_ms::Union{Int64, Nothing}
    query_options_include_instance_types::Union{Bool, Nothing}
    query_options_prefetch_size::Union{Int64, Nothing}
    query_options_include_query_structure::Union{Bool, Nothing}
end

const CTX = BehaviourContext(
    nothing, nothing,
    Transaction[], Transaction[], Task[],
    nothing, nothing,
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
    CTX.query_answer_rows = collect(rows(CTX.query_answer))
end

"""Execute a typeql query on the current transaction, applying stored query options."""
function _run_query(typeql::AbstractString)::QueryAnswer
    query(CTX.transaction, typeql;
        include_instance_types  = CTX.query_options_include_instance_types,
        prefetch_size           = CTX.query_options_prefetch_size,
        include_query_structure = CTX.query_options_include_query_structure)
end
