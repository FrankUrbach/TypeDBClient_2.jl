# ─── Concept ─────────────────────────────────────────────────────────────────

"""
    Concept

Wraps a native TypeDB concept handle.  Concepts represent entities, relations,
attributes, types, or raw values returned by a query.

Always obtain `Concept` objects from a [`ConceptRow`](@ref) via
[`get_concept`](@ref) or by iterating a [`ConceptRow`](@ref).
"""
mutable struct Concept
    handle::ConceptHandle
    _dropped::Bool

    function Concept(handle::ConceptHandle)
        handle == C_NULL && error("Concept handle is NULL")
        obj = new(handle, false)
        finalizer(obj) do c
            c._dropped || (c._dropped = true; FFI.concept_drop(c.handle))
        end
        obj
    end
end

# ─── Kind predicates ─────────────────────────────────────────────────────────

is_entity_type(c::Concept)    = FFI.concept_is_entity_type(c.handle)
is_relation_type(c::Concept)  = FFI.concept_is_relation_type(c.handle)
is_attribute_type(c::Concept) = FFI.concept_is_attribute_type(c.handle)
is_role_type(c::Concept)      = FFI.concept_is_role_type(c.handle)
is_entity(c::Concept)         = FFI.concept_is_entity(c.handle)
is_relation(c::Concept)       = FFI.concept_is_relation(c.handle)
is_attribute(c::Concept)      = FFI.concept_is_attribute(c.handle)
is_value(c::Concept)          = FFI.concept_is_value(c.handle)

# ─── Label / IID ─────────────────────────────────────────────────────────────

"""
    get_label(concept) -> String

Return the type label of the concept.
"""
function get_label(c::Concept)::String
    cstr = FFI.concept_get_label(c.handle)
    typedb_string(cstr)
end

"""
    try_get_label(concept) -> Union{String,Nothing}
"""
function try_get_label(c::Concept)::Union{String,Nothing}
    cstr = FFI.concept_try_get_label(c.handle)
    cstr == C_NULL ? nothing : typedb_string(cstr)
end

"""
    try_get_iid(concept) -> Union{String,Nothing}

Return the IID of an entity or relation instance, or `nothing` for types/values.
"""
function try_get_iid(c::Concept)::Union{String,Nothing}
    cstr = FFI.concept_try_get_iid(c.handle)
    cstr == C_NULL ? nothing : typedb_owned_string(cstr)
end

"""
    try_get_value_type(concept) -> Union{String,Nothing}

Return the value type string (e.g. `"string"`, `"integer"`, `"double"`) for
attribute-type or value concepts.
"""
function try_get_value_type(c::Concept)::Union{String,Nothing}
    cstr = FFI.concept_try_get_value_type(c.handle)
    cstr == C_NULL ? nothing : typedb_owned_string(cstr)
end

# ─── Value extraction ─────────────────────────────────────────────────────────

"""
    get_value(concept) -> Union{Bool, Int64, Float64, String, Nothing}

Extract the primitive value from a concept that represents an attribute value
or a raw value.  Returns `nothing` if the concept has no value.
"""
function get_value(c::Concept)::Union{Bool, Int64, Float64, String, Nothing}
    h = c.handle
    FFI.concept_is_boolean(h)  && return FFI.concept_get_boolean(h)
    FFI.concept_is_integer(h)  && return FFI.concept_get_integer(h)
    FFI.concept_is_double(h)   && return FFI.concept_get_double(h)
    if FFI.concept_is_string(h)
        cstr = FFI.concept_get_string(h)
        return typedb_owned_string(cstr)
    end
    if FFI.concept_is_date(h)
        secs = FFI.concept_get_date_as_seconds(h)
        return secs   # caller can convert to Date if needed
    end
    # Try attribute value
    val_ptr = FFI.concept_try_get_value(h)
    val_ptr == C_NULL && return nothing
    val_concept = Concept(val_ptr)
    get_value(val_concept)
end

"""
    concept_to_string(concept) -> String

Return the human-readable string representation of the concept.
"""
function concept_to_string(c::Concept)::String
    cstr = FFI.concept_to_string(c.handle)
    typedb_owned_string(cstr)
end

Base.show(io::IO, c::Concept) = print(io, "Concept($(concept_to_string(c)))")

# ─── ConceptRow ───────────────────────────────────────────────────────────────

"""
    ConceptRow

A single row of query results, mapping column names to [`Concept`](@ref) objects.

Obtain via iteration of a [`ConceptRowIterator`](@ref).
"""
mutable struct ConceptRow
    handle::ConceptRowHandle
    _dropped::Bool

    function ConceptRow(handle::ConceptRowHandle)
        handle == C_NULL && error("ConceptRow handle is NULL")
        obj = new(handle, false)
        finalizer(obj) do r
            r._dropped || (r._dropped = true; FFI.concept_row_drop(r.handle))
        end
        obj
    end
end

"""
    get_concept(row, column_name) -> Concept

Retrieve the concept at the named column.  Throws if the column is absent.
"""
function get_concept(row::ConceptRow, col::AbstractString)::Concept
    h = GC.@preserve col begin
        FFI.concept_row_get(
            row.handle,
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, col)))
    end
    check_and_throw()
    h == C_NULL && error("Column \"$col\" not found in ConceptRow")
    Concept(h)
end

"""
    get_concept(row, index::Integer) -> Concept

Retrieve the concept at the given zero-based column index.
"""
function get_concept(row::ConceptRow, idx::Integer)::Concept
    h = FFI.concept_row_get_index(row.handle, Csize_t(idx))
    check_and_throw()
    h == C_NULL && error("Column index $idx out of range")
    Concept(h)
end

"""
    column_names(row) -> Vector{String}

Return the column names of the row.
"""
function column_names(row::ConceptRow)::Vector{String}
    iter = FFI.concept_row_get_column_names(row.handle)
    iter == C_NULL && return String[]
    names = String[]
    try
        while true
            cstr = FFI.string_iterator_next(iter)
            check_and_throw()
            cstr == C_NULL && break
            push!(names, typedb_string(cstr))
        end
    finally
        FFI.string_iterator_drop(iter)
    end
    names
end

function Base.show(io::IO, row::ConceptRow)
    cstr = FFI.concept_row_to_string(row.handle)
    s = typedb_owned_string(cstr)
    print(io, "ConceptRow(", s, ")")
end

# ─── ConceptRowIterator ───────────────────────────────────────────────────────

"""
    ConceptRowIterator

A Julia iterator over [`ConceptRow`](@ref) objects returned by a row-based query.
"""
mutable struct ConceptRowIterator
    handle::ConceptRowIterHandle
    _done::Bool

    function ConceptRowIterator(handle::ConceptRowIterHandle)
        handle == C_NULL && error("ConceptRowIterator handle is NULL")
        obj = new(handle, false)
        finalizer(obj) do it
            it._done || (it._done = true; FFI.concept_row_iterator_drop(it.handle))
        end
        obj
    end
end

function Base.iterate(iter::ConceptRowIterator, ::Nothing=nothing)
    iter._done && return nothing
    h = FFI.concept_row_iterator_next(iter.handle)
    check_and_throw()
    if h == C_NULL
        iter._done = true
        return nothing
    end
    return (ConceptRow(h), nothing)
end

Base.IteratorSize(::Type{ConceptRowIterator}) = Base.SizeUnknown()
Base.eltype(::Type{ConceptRowIterator}) = ConceptRow

# ─── DocumentIterator (fetch queries returning JSON strings) ──────────────────

"""
    DocumentIterator

A Julia iterator over JSON document strings returned by a document-based query
(e.g. `fetch` queries).
"""
mutable struct DocumentIterator
    handle::StringIterHandle
    _done::Bool

    function DocumentIterator(handle::StringIterHandle)
        handle == C_NULL && error("DocumentIterator handle is NULL")
        obj = new(handle, false)
        finalizer(obj) do it
            it._done || (it._done = true; FFI.string_iterator_drop(it.handle))
        end
        obj
    end
end

function Base.iterate(iter::DocumentIterator, ::Nothing=nothing)
    iter._done && return nothing
    cstr = FFI.string_iterator_next(iter.handle)
    check_and_throw()
    if cstr == C_NULL
        iter._done = true
        return nothing
    end
    return (typedb_owned_string(cstr), nothing)
end

Base.IteratorSize(::Type{DocumentIterator}) = Base.SizeUnknown()
Base.eltype(::Type{DocumentIterator}) = String

# ─── QueryAnswer ─────────────────────────────────────────────────────────────

"""
    QueryAnswer

The result of a TypeDB query.  Depending on the query type it may contain:
- A row stream  (`match … fetch` or `match … select`): iterate with [`rows`](@ref)
- A document stream (`fetch`): iterate with [`documents`](@ref)
- A success flag (`insert`, `delete`, schema queries): check with [`is_ok`](@ref)
"""
mutable struct QueryAnswer
    handle::QueryAnswerHandle
    _consumed::Bool

    function QueryAnswer(handle::QueryAnswerHandle)
        handle == C_NULL && error("QueryAnswer handle is NULL")
        obj = new(handle, false)
        finalizer(obj) do qa
            qa._consumed || (qa._consumed = true; FFI.query_answer_drop(qa.handle))
        end
        obj
    end
end

"""
    is_ok(answer::QueryAnswer) -> Bool
"""
is_ok(qa::QueryAnswer) = FFI.query_answer_is_ok(qa.handle)

"""
    is_row_stream(answer::QueryAnswer) -> Bool
"""
is_row_stream(qa::QueryAnswer) = FFI.query_answer_is_concept_row_stream(qa.handle)

"""
    is_document_stream(answer::QueryAnswer) -> Bool
"""
is_document_stream(qa::QueryAnswer) = FFI.query_answer_is_concept_document_stream(qa.handle)

"""
    rows(answer::QueryAnswer) -> ConceptRowIterator

Convert a row-stream answer into a Julia iterator.  Can only be called once
per answer (consuming the handle).
"""
function rows(qa::QueryAnswer)::ConceptRowIterator
    qa._consumed && error("QueryAnswer already consumed")
    qa._consumed = true
    h = FFI.query_answer_into_rows(qa.handle)
    check_and_throw()
    ConceptRowIterator(h)
end

"""
    documents(answer::QueryAnswer) -> DocumentIterator

Convert a document-stream answer into a Julia iterator.  Can only be called
once per answer.
"""
function documents(qa::QueryAnswer)::DocumentIterator
    qa._consumed && error("QueryAnswer already consumed")
    qa._consumed = true
    h = FFI.query_answer_into_documents(qa.handle)
    check_and_throw()
    DocumentIterator(h)
end
