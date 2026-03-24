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
                FFI.transaction_drop_sync(tx.handle)
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
    # transaction_commit uses take_ownership() internally: the native Transaction object
    # is freed by Rust immediately, regardless of whether the commit succeeds or fails.
    # Mark _closed NOW so that no subsequent code (finalizer, _close_sync, isopen) tries
    # to use tx.handle again — doing so would be a use-after-free or double-free.
    tx._closed = true
    FFI.void_promise_resolve(prom)
    check_and_throw()
    tx._committed = true
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
    # transaction_drop_sync closes (no-op if already closed) and frees the handle.
    # Safe to call even when transaction_is_open returns false (e.g. after rollback).
    FFI.transaction_drop_sync(tx.handle)
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

# ─── Public low-level open ────────────────────────────────────────────────────

"""
    open_transaction(driver, db_name, tx_type; timeout_ms, schema_lock_ms) -> Transaction

Open a transaction directly (without a do-block).  The caller is responsible
for calling `commit`, `rollback`, or `close` when done.

```julia
tx = open_transaction(driver, "mydb", TransactionType.READ)
# ... use tx ...
close(tx)
```
"""
function open_transaction(driver::TypeDBDriver, db_name::AbstractString,
                          tx_type::Int32;
                          timeout_ms::Union{Int64,Nothing}      = nothing,
                          schema_lock_ms::Union{Int64,Nothing}  = nothing)::Transaction
    _open_transaction(driver, db_name, tx_type, timeout_ms, schema_lock_ms)
end

"""
    close(tx::Transaction)

Close the transaction without committing.  Idempotent.
"""
Base.close(tx::Transaction) = _close_sync(tx)

"""
    transaction_type_name(tx::Transaction) -> String

Return the transaction type as a lower-case string: `"read"`, `"write"`, or `"schema"`.
"""
function transaction_type_name(tx::Transaction)::String
    if tx.tx_type == TransactionType.READ
        return "read"
    elseif tx.tx_type == TransactionType.WRITE
        return "write"
    elseif tx.tx_type == TransactionType.SCHEMA
        return "schema"
    else
        return "unknown"
    end
end
