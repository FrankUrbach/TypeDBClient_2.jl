# ─── Query execution ──────────────────────────────────────────────────────────

"""
    query(tx::Transaction, typeql::AbstractString; kwargs...) -> QueryAnswer

Execute a TypeQL query within an open transaction and return the result as a
[`QueryAnswer`](@ref).

Keyword arguments (all optional):
- `include_instance_types::Bool`  – include type information for instances
- `prefetch_size::Int64`          – number of results to prefetch
- `include_query_structure::Bool` – attach the parsed query structure to rows

Depending on the query type:
- **Read queries** (`match … select/fetch`) → iterate with `rows(answer)` or `documents(answer)`
- **Write / schema queries**                → check success with `is_ok(answer)`

```julia
# Fetch rows
answer = query(tx, "match \$p isa person; select \$p;")
for row in rows(answer)
    println(get_concept(row, "p"))
end

# Insert
query(tx, "insert \$x isa person, has name \\"Alice\\";")

# Fetch JSON documents
answer = query(tx, "match \$p isa person; fetch \$p: name;")
for doc in documents(answer)
    println(doc)
end
```
"""
function query(tx::Transaction, typeql::AbstractString;
               include_instance_types::Union{Bool,Nothing}    = nothing,
               prefetch_size::Union{Int64,Nothing}            = nothing,
               include_query_structure::Union{Bool,Nothing}   = nothing)::QueryAnswer

    isopen(tx) || error("Transaction is not open")

    opts = FFI.query_options_new()
    include_instance_types  === nothing || FFI.query_options_set_include_instance_types(opts, include_instance_types)
    prefetch_size           === nothing || FFI.query_options_set_prefetch_size(opts, prefetch_size)
    include_query_structure === nothing || FFI.query_options_set_include_query_structure(opts, include_query_structure)

    prom = GC.@preserve typeql begin
        @checkerr FFI.transaction_query(
            tx.handle,
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, typeql)),
            opts)
    end

    FFI.query_options_drop(opts)

    ans_handle = FFI.query_answer_promise_resolve(prom)
    check_and_throw()
    QueryAnswer(ans_handle)
end
