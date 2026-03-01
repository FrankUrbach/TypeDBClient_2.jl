# ─── Transaction struct ───────────────────────────────────────────────────────

"""
    Transaction

Represents an open TypeDB transaction.  Wraps the native transaction handle
and provides RAII-style cleanup via a finalizer.

Use the do-block form via [`transaction`](@ref) for automatic commit/rollback.

Note: TypeDB 3.x no longer has a separate *session* concept; transactions are
opened directly against a database by name.
"""
mutable struct Transaction
    driver::TypeDBDriver    # keep driver alive
    db_name::String
    tx_type::Int32
    handle::TransactionHandle
    _committed::Bool
    _closed::Bool

    function Transaction(driver::TypeDBDriver, db_name::String,
                         tx_type::Int32, handle::TransactionHandle)
        handle == C_NULL && error("transaction_new returned NULL")
        obj = new(driver, db_name, tx_type, handle, false, false)
        finalizer(obj) do tx
            if !tx._closed
                tx._closed = true
                if FFI.transaction_is_open(tx.handle)
                    FFI.transaction_drop_sync(tx.handle)
                end
            end
        end
        obj
    end
end

Base.show(io::IO, tx::Transaction) =
    print(io, "Transaction($(tx.db_name), type=$(tx.tx_type), open=$(isopen(tx)))")

Base.isopen(tx::Transaction) =
    !tx._closed && FFI.transaction_is_open(tx.handle)

# ─── Low-level open ───────────────────────────────────────────────────────────

function _open_transaction(driver::TypeDBDriver, db_name::AbstractString,
                           tx_type::Int32,
                           timeout_ms::Union{Int64,Nothing}=nothing,
                           schema_lock_ms::Union{Int64,Nothing}=nothing)::Transaction

    opts = FFI.transaction_options_new()
    timeout_ms    === nothing || FFI.transaction_options_set_transaction_timeout_millis(opts, timeout_ms)
    schema_lock_ms === nothing || FFI.transaction_options_set_schema_lock_acquire_timeout_millis(opts, schema_lock_ms)

    handle = GC.@preserve db_name begin
        @checkerr FFI.transaction_new(
            driver.handle,
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, db_name)),
            tx_type, opts)
    end

    FFI.transaction_options_drop(opts)
    Transaction(driver, String(db_name), tx_type, handle)
end

# ─── Commit / rollback ────────────────────────────────────────────────────────

"""
    commit(tx::Transaction)

Commit the transaction.  Throws on error.
"""
function commit(tx::Transaction)
    tx._committed && error("Transaction already committed")
    tx._closed && error("Transaction already closed")
    prom = FFI.transaction_commit(tx.handle)
    FFI.void_promise_resolve(prom)
    check_and_throw()
    tx._committed = true
    tx._closed    = true
    nothing
end

"""
    rollback(tx::Transaction)

Roll back all changes made in the transaction.
"""
function rollback(tx::Transaction)
    tx._closed && return
    prom = FFI.transaction_rollback(tx.handle)
    FFI.void_promise_resolve(prom)
    check_and_throw()
    nothing
end

function _close_sync(tx::Transaction)
    tx._closed && return
    tx._closed = true
    if FFI.transaction_is_open(tx.handle)
        FFI.transaction_drop_sync(tx.handle)
    end
end

# ─── High-level do-block API ─────────────────────────────────────────────────

"""
    transaction(f, driver, db_name, tx_type; timeout_ms, schema_lock_ms)
    transaction(f, db::Database, tx_type; timeout_ms, schema_lock_ms)

Open a transaction, execute `f(tx)`, then:
- **WRITE / SCHEMA**: auto-commit if `f` returns without throwing.
- **READ**: close without committing.
- On any exception: roll back (if transaction is still open), then rethrow.

```julia
transaction(driver, "mydb", TransactionType.WRITE) do tx
    query(tx, "insert \$x isa person, has name \\"Alice\\";")
end  # committed automatically

transaction(driver, "mydb", TransactionType.READ) do tx
    for row in query(tx, "match \$p isa person; fetch \$p: name;")
        println(get_value(get_concept(row, "p"), "name"))
    end
end
```
"""
function transaction(f::Function, driver::TypeDBDriver, db_name::AbstractString,
                     tx_type::Int32;
                     timeout_ms::Union{Int64,Nothing}       = nothing,
                     schema_lock_ms::Union{Int64,Nothing}   = nothing)

    tx = _open_transaction(driver, db_name, tx_type, timeout_ms, schema_lock_ms)
    committed = false
    try
        result = f(tx)
        if tx_type != TransactionType.READ && isopen(tx)
            commit(tx)
            committed = true
        end
        return result
    catch ex
        if !committed && isopen(tx)
            try rollback(tx) catch; end
        end
        rethrow(ex)
    finally
        _close_sync(tx)
    end
end

function transaction(f::Function, db::Database, tx_type::Int32; kwargs...)
    transaction(f, db.driver, db._name, tx_type; kwargs...)
end
