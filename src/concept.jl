# ─── Abstract type hierarchy ──────────────────────────────────────────────────

"""
    TypeDBValue

Abstract base for all TypeDB value types.

Concrete subtypes: [`BooleanValue`](@ref), [`IntegerValue`](@ref),
[`DoubleValue`](@ref), [`DecimalValue`](@ref), [`StringValue`](@ref),
[`DateValue`](@ref), [`DatetimeValue`](@ref), [`DatetimeTZValue`](@ref),
[`DurationValue`](@ref), [`StructValue`](@ref).
"""
abstract type TypeDBValue end

"""
    TypeDBType

Abstract base for all TypeDB schema types.

Concrete subtypes: [`EntityType`](@ref), [`RelationType`](@ref),
[`AttributeType`](@ref), [`RoleType`](@ref).
"""
abstract type TypeDBType end

"""
    TypeDBInstance

Abstract base for all TypeDB data instances.

Concrete subtypes: [`Entity`](@ref), [`Relation`](@ref), [`Attribute`](@ref).
"""
abstract type TypeDBInstance end

# ─── Value helper types ───────────────────────────────────────────────────────

"""
    Decimal

Fixed-point number with 19 fractional decimal digits, matching TypeDB's
`decimal` value type.  The actual value is `integer + fractional × 10⁻¹⁹`.
"""
struct Decimal
    integer::Int64
    fractional::UInt64
end

"""
    TypeDBDuration

Relative duration containing months, days, and nanoseconds, matching
TypeDB's `duration` value type (ISO-8601 compliant).
"""
struct TypeDBDuration
    months::UInt32
    days::UInt32
    nanos::UInt64
end

"""
    TimeZoneSpec

Abstract base for timezone representations.  Either [`IANATimeZone`](@ref)
or [`FixedTimeZone`](@ref).
"""
abstract type TimeZoneSpec end

"IANA/Olsen timezone by name, e.g. `IANATimeZone(\"Europe/Berlin\")`."
struct IANATimeZone <: TimeZoneSpec
    name::String
end

"Fixed UTC offset in seconds, e.g. `FixedTimeZone(3600)` for UTC+01:00."
struct FixedTimeZone <: TimeZoneSpec
    offset_seconds::Int32
end

"""
    DatetimeTZ

A datetime together with its timezone.  `datetime` holds the local
wall-clock time as a `Dates.DateTime`; `timezone` is an [`IANATimeZone`](@ref)
or [`FixedTimeZone`](@ref).
"""
struct DatetimeTZ
    datetime::Dates.DateTime
    timezone::TimeZoneSpec
end

# ─── Concrete value types ─────────────────────────────────────────────────────

"TypeDB `boolean` value."
struct BooleanValue <: TypeDBValue
    value::Bool
end

"TypeDB `integer` value (64-bit signed)."
struct IntegerValue <: TypeDBValue
    value::Int64
end

"TypeDB `double` value (64-bit float)."
struct DoubleValue <: TypeDBValue
    value::Float64
end

"TypeDB `decimal` value (fixed-point with 19 fractional digits)."
struct DecimalValue <: TypeDBValue
    value::Decimal
end

"TypeDB `string` value."
struct StringValue <: TypeDBValue
    value::String
end

"TypeDB `date` value (calendar date without time)."
struct DateValue <: TypeDBValue
    value::Dates.Date
end

"TypeDB `datetime` value (no timezone, millisecond resolution)."
struct DatetimeValue <: TypeDBValue
    value::Dates.DateTime
end

"TypeDB `datetime-tz` value (datetime + timezone)."
struct DatetimeTZValue <: TypeDBValue
    value::DatetimeTZ
end

"TypeDB `duration` value."
struct DurationValue <: TypeDBValue
    value::TypeDBDuration
end

"""
    StructValue

TypeDB `struct` value — a named collection of typed fields.
Fields may be absent (`nothing`) for optional struct fields.
"""
struct StructValue <: TypeDBValue
    fields::Dict{String, Union{Nothing, TypeDBValue}}
end

# ─── Schema types ─────────────────────────────────────────────────────────────

"A TypeDB entity type (classifies independent objects)."
struct EntityType <: TypeDBType
    label::String
end

"A TypeDB relation type (classifies relationships between typed participants)."
struct RelationType <: TypeDBType
    label::String
end

"""
    AttributeType(label, value_type)

A TypeDB attribute type.  `value_type` is the permitted value kind
(`"boolean"`, `"integer"`, `"double"`, `"decimal"`, `"string"`, `"date"`,
`"datetime"`, `"datetime-tz"`, `"duration"`, or a struct name), or
`nothing` for abstract attribute types.
"""
struct AttributeType <: TypeDBType
    label::String
    value_type::Union{String, Nothing}
end

"A TypeDB role type (used by relations to declare participant slots)."
struct RoleType <: TypeDBType
    label::String
end

# ─── Instance types ───────────────────────────────────────────────────────────

"""
    Entity(iid, type_)

A TypeDB entity instance.  `iid` is the instance identifier string;
`type_` is an [`EntityType`](@ref), or `nothing` when type information
was not fetched (see query option `include_instance_types`).
"""
struct Entity <: TypeDBInstance
    iid::String
    type_::Union{EntityType, Nothing}
end

"A TypeDB relation instance."
struct Relation <: TypeDBInstance
    iid::String
    type_::Union{RelationType, Nothing}
end

"""
    Attribute(iid, value, type_)

A TypeDB attribute instance, carrying a typed [`TypeDBValue`](@ref).
"""
struct Attribute <: TypeDBInstance
    iid::String
    value::TypeDBValue
    type_::Union{AttributeType, Nothing}
end

# ─── Concept union ────────────────────────────────────────────────────────────

"""
    Concept

Union of all concrete TypeDB concept types.

```
Concept = Union{TypeDBType, TypeDBInstance, TypeDBValue}
```

Any [`EntityType`](@ref), [`RelationType`](@ref), [`AttributeType`](@ref),
[`RoleType`](@ref), [`Entity`](@ref), [`Relation`](@ref), [`Attribute`](@ref),
or [`TypeDBValue`](@ref) subtype is `isa Concept`.
"""
const Concept = Union{TypeDBType, TypeDBInstance, TypeDBValue}

# ─── Private materialization helpers ─────────────────────────────────────────

function _concept_iid(h::ConceptHandle)::String
    cstr = FFI.concept_try_get_iid(h)
    cstr == C_NULL ? "" : typedb_owned_string(cstr)
end

function _entity_type_of(h::ConceptHandle)::Union{EntityType, Nothing}
    type_h = FFI.entity_get_type(h)
    type_h == C_NULL && return nothing
    label = typedb_owned_string(FFI.concept_get_label(type_h))
    FFI.concept_drop(type_h)
    EntityType(label)
end

function _relation_type_of(h::ConceptHandle)::Union{RelationType, Nothing}
    type_h = FFI.relation_get_type(h)
    type_h == C_NULL && return nothing
    label = typedb_owned_string(FFI.concept_get_label(type_h))
    FFI.concept_drop(type_h)
    RelationType(label)
end

function _attribute_type_of(h::ConceptHandle)::Union{AttributeType, Nothing}
    type_h = FFI.attribute_get_type(h)
    type_h == C_NULL && return nothing
    label = typedb_owned_string(FFI.concept_get_label(type_h))
    vt_cstr = FFI.concept_try_get_value_type(type_h)
    vt = vt_cstr == C_NULL ? nothing : typedb_owned_string(vt_cstr)
    FFI.concept_drop(type_h)
    AttributeType(label, vt)
end

function _cnanos_to_datetime(cn::FFI.DatetimeInNanos)::Dates.DateTime
    # cn.seconds = Unix timestamp seconds, cn.subsec_nanos = sub-second nanoseconds.
    # Julia's DateTime has millisecond resolution; truncate sub-millisecond precision.
    ms = Int64(cn.seconds) * 1000 + Int64(cn.subsec_nanos) ÷ 1_000_000
    Dates.DateTime(1970, 1, 1) + Dates.Millisecond(ms)
end

function _extract_datetime_tz(dtz::FFI.DatetimeAndTimeZone)::DatetimeTZValue
    dt = _cnanos_to_datetime(dtz.datetime_in_nanos)
    tz = if dtz.is_fixed_offset
        FixedTimeZone(dtz.local_minus_utc_offset)
    else
        IANATimeZone(dtz.zone_name == C_NULL ? "" : unsafe_string(dtz.zone_name))
    end
    # Free the Rust-allocated zone_name (non-null even for fixed-offset entries).
    dtz.zone_name != C_NULL && FFI.string_free(dtz.zone_name)
    DatetimeTZValue(DatetimeTZ(dt, tz))
end

function _extract_struct_fields(h::ConceptHandle)::Dict{String, Union{Nothing, TypeDBValue}}
    iter_h = FFI.concept_get_struct(h)
    check_and_throw()
    fields = Dict{String, Union{Nothing, TypeDBValue}}()
    iter_h == C_NULL && return fields
    try
        while true
            sav_ptr = FFI.string_and_opt_value_iterator_next(iter_h)
            check_and_throw()
            sav_ptr == C_NULL && break
            sav = unsafe_load(Ptr{FFI.StringAndOptValue}(sav_ptr))
            field_name  = unsafe_string(sav.string_ptr)
            field_value = sav.value_ptr == C_NULL ? nothing : _extract_value(sav.value_ptr)
            fields[field_name] = field_value
            # string_and_opt_value_drop frees both string_ptr and value_ptr via Rust Drop.
            FFI.string_and_opt_value_drop(sav_ptr)
        end
    finally
        FFI.string_and_opt_value_iterator_drop(iter_h)
    end
    fields
end

"""
    _extract_value(h::ConceptHandle) -> TypeDBValue

Extract a typed Julia value from a concept handle that holds a value.
Does **not** drop `h`; the caller remains responsible for `FFI.concept_drop`.
"""
function _extract_value(h::ConceptHandle)::TypeDBValue
    FFI.concept_is_boolean(h)  && return BooleanValue(FFI.concept_get_boolean(h))
    FFI.concept_is_integer(h)  && return IntegerValue(FFI.concept_get_integer(h))
    FFI.concept_is_double(h)   && return DoubleValue(FFI.concept_get_double(h))
    if FFI.concept_is_decimal(h)
        d = FFI.concept_get_decimal(h)
        return DecimalValue(Decimal(d.integer, d.fractional))
    end
    if FFI.concept_is_string(h)
        return StringValue(typedb_owned_string(FFI.concept_get_string(h)))
    end
    if FFI.concept_is_date(h)
        secs = FFI.concept_get_date_as_seconds(h)
        return DateValue(Dates.Date(Dates.DateTime(1970, 1, 1) + Dates.Second(secs)))
    end
    if FFI.concept_is_datetime(h)
        return DatetimeValue(_cnanos_to_datetime(FFI.concept_get_datetime(h)))
    end
    if FFI.concept_is_datetime_tz(h)
        return _extract_datetime_tz(FFI.concept_get_datetime_tz(h))
    end
    if FFI.concept_is_duration(h)
        dur = FFI.concept_get_duration(h)
        return DurationValue(TypeDBDuration(dur.months, dur.days, dur.nanos))
    end
    if FFI.concept_is_struct(h)
        return StructValue(_extract_struct_fields(h))
    end
    error("_extract_value: unknown value kind for concept handle")
end

"""
    materialize(h::ConceptHandle) -> Concept

Convert a raw C concept handle into a rich Julia concept value.
Takes ownership of `h` — always calls `FFI.concept_drop` before returning,
even if an error is thrown.
"""
function materialize(h::ConceptHandle)::Concept
    try
        if FFI.concept_is_entity_type(h)
            return EntityType(typedb_owned_string(FFI.concept_get_label(h)))

        elseif FFI.concept_is_relation_type(h)
            return RelationType(typedb_owned_string(FFI.concept_get_label(h)))

        elseif FFI.concept_is_attribute_type(h)
            label    = typedb_owned_string(FFI.concept_get_label(h))
            vt_cstr  = FFI.concept_try_get_value_type(h)
            vt       = vt_cstr == C_NULL ? nothing : typedb_owned_string(vt_cstr)
            return AttributeType(label, vt)

        elseif FFI.concept_is_role_type(h)
            return RoleType(typedb_owned_string(FFI.concept_get_label(h)))

        elseif FFI.concept_is_entity(h)
            return Entity(_concept_iid(h), _entity_type_of(h))

        elseif FFI.concept_is_relation(h)
            return Relation(_concept_iid(h), _relation_type_of(h))

        elseif FFI.concept_is_attribute(h)
            iid   = _concept_iid(h)
            type_ = _attribute_type_of(h)
            val_h = FFI.concept_try_get_value(h)
            check_and_throw()
            val_h == C_NULL && error("Attribute concept carries no value")
            value = _extract_value(val_h)
            FFI.concept_drop(val_h)
            return Attribute(iid, value, type_)

        elseif FFI.concept_is_value(h)
            return _extract_value(h)

        else
            error("materialize: unknown concept kind")
        end
    finally
        FFI.concept_drop(h)
    end
end

# ─── Kind predicates (dispatch-based, no C API call needed) ──────────────────

"""    is_entity_type(c) -> Bool"""
is_entity_type(::EntityType) = true
is_entity_type(::Any)        = false

"""    is_relation_type(c) -> Bool"""
is_relation_type(::RelationType) = true
is_relation_type(::Any)          = false

"""    is_attribute_type(c) -> Bool"""
is_attribute_type(::AttributeType) = true
is_attribute_type(::Any)           = false

"""    is_role_type(c) -> Bool"""
is_role_type(::RoleType) = true
is_role_type(::Any)      = false

"""    is_entity(c) -> Bool"""
is_entity(::Entity) = true
is_entity(::Any)    = false

"""    is_relation(c) -> Bool"""
is_relation(::Relation) = true
is_relation(::Any)      = false

"""    is_attribute(c) -> Bool"""
is_attribute(::Attribute) = true
is_attribute(::Any)       = false

"""    is_value(c) -> Bool"""
is_value(::TypeDBValue) = true
is_value(::Any)         = false

"""    is_type(c) -> Bool — true for any schema type."""
is_type(::TypeDBType) = true
is_type(::Any)        = false

"""    is_instance(c) -> Bool — true for entity, relation, and attribute instances."""
is_instance(::TypeDBInstance) = true
is_instance(::Any)            = false

# ─── Value-kind predicates ────────────────────────────────────────────────────

"""    is_boolean(c) -> Bool"""
is_boolean(::BooleanValue) = true;  is_boolean(::Any) = false

"""    is_integer(c) -> Bool"""
is_integer(::IntegerValue) = true;  is_integer(::Any) = false

"""    is_double(c) -> Bool"""
is_double(::DoubleValue)   = true;  is_double(::Any)  = false

"""    is_decimal(c) -> Bool"""
is_decimal(::DecimalValue) = true;  is_decimal(::Any) = false

"""    is_string_value(c) -> Bool"""
is_string_value(::StringValue) = true;  is_string_value(::Any) = false

"""    is_date(c) -> Bool"""
is_date(::DateValue)       = true;  is_date(::Any)    = false

"""    is_datetime(c) -> Bool"""
is_datetime(::DatetimeValue) = true;  is_datetime(::Any) = false

"""    is_datetime_tz(c) -> Bool"""
is_datetime_tz(::DatetimeTZValue) = true;  is_datetime_tz(::Any) = false

"""    is_duration(c) -> Bool"""
is_duration(::DurationValue) = true;  is_duration(::Any) = false

"""    is_struct_value(c) -> Bool"""
is_struct_value(::StructValue) = true;  is_struct_value(::Any) = false

# Forwarding: Attribute delegates value-kind predicates to its wrapped value.
is_boolean(a::Attribute)     = is_boolean(a.value)
is_integer(a::Attribute)     = is_integer(a.value)
is_double(a::Attribute)      = is_double(a.value)
is_decimal(a::Attribute)      = is_decimal(a.value)
is_string_value(a::Attribute) = is_string_value(a.value)
is_date(a::Attribute)         = is_date(a.value)
is_datetime(a::Attribute)    = is_datetime(a.value)
is_datetime_tz(a::Attribute) = is_datetime_tz(a.value)
is_duration(a::Attribute)    = is_duration(a.value)
is_struct_value(a::Attribute) = is_struct_value(a.value)

# ─── Label / IID / value-type accessors ──────────────────────────────────────

_value_type_label(::BooleanValue)    = "boolean"
_value_type_label(::IntegerValue)    = "integer"
_value_type_label(::DoubleValue)     = "double"
_value_type_label(::DecimalValue)    = "decimal"
_value_type_label(::StringValue)     = "string"
_value_type_label(::DateValue)       = "date"
_value_type_label(::DatetimeValue)   = "datetime"
_value_type_label(::DatetimeTZValue) = "datetime-tz"
_value_type_label(::DurationValue)   = "duration"
_value_type_label(::StructValue)     = "struct"

"""
    get_label(c) -> String

Return the type label of a schema type, the type label of an instance,
or the value-type label of a value concept.
"""
get_label(c::TypeDBType)   = c.label
get_label(c::Entity)       = isnothing(c.type_) ? "unknown" : c.type_.label
get_label(c::Relation)     = isnothing(c.type_) ? "unknown" : c.type_.label
get_label(c::Attribute)    = isnothing(c.type_) ? "unknown" : c.type_.label
get_label(c::TypeDBValue)  = _value_type_label(c)

"""
    try_get_label(c) -> Union{String, Nothing}

Like [`get_label`](@ref) but returns `nothing` when the label is unavailable.
"""
try_get_label(c::TypeDBType)   = c.label
try_get_label(c::Entity)       = isnothing(c.type_) ? nothing : c.type_.label
try_get_label(c::Relation)     = isnothing(c.type_) ? nothing : c.type_.label
try_get_label(c::Attribute)    = isnothing(c.type_) ? nothing : c.type_.label
try_get_label(c::TypeDBValue)  = _value_type_label(c)

"""
    try_get_iid(c) -> Union{String, Nothing}

Return the IID of an entity or relation instance, or `nothing` for types/values.
"""
try_get_iid(c::TypeDBInstance) = c.iid
try_get_iid(::Any)             = nothing

"""
    try_get_value_type(c) -> Union{String, Nothing}

Return the value-type string for attribute types, attribute instances, or values.
Returns `nothing` for entities, relations, and untyped attribute types.
"""
try_get_value_type(c::AttributeType) = c.value_type
try_get_value_type(c::Attribute)     = isnothing(c.type_) ? nothing : c.type_.value_type
try_get_value_type(c::TypeDBValue)   = _value_type_label(c)
try_get_value_type(::Any)            = nothing

# ─── Value extractor ─────────────────────────────────────────────────────────

"""
    get_value(c) -> value or nothing

Extract the underlying Julia value from a [`TypeDBValue`](@ref) or [`Attribute`](@ref).

| Concept type      | Return type              |
|-------------------|--------------------------|
| `BooleanValue`    | `Bool`                   |
| `IntegerValue`    | `Int64`                  |
| `DoubleValue`     | `Float64`                |
| `DecimalValue`    | [`Decimal`](@ref)        |
| `StringValue`     | `String`                 |
| `DateValue`       | `Dates.Date`             |
| `DatetimeValue`   | `Dates.DateTime`         |
| `DatetimeTZValue` | [`DatetimeTZ`](@ref)     |
| `DurationValue`   | [`TypeDBDuration`](@ref) |
| `StructValue`     | `Dict{String,…}`         |
| `Attribute`       | delegates to its value   |
| anything else     | `nothing`                |
"""
get_value(v::BooleanValue)    = v.value
get_value(v::IntegerValue)    = v.value
get_value(v::DoubleValue)     = v.value
get_value(v::DecimalValue)    = v.value
get_value(v::StringValue)     = v.value
get_value(v::DateValue)       = v.value
get_value(v::DatetimeValue)   = v.value
get_value(v::DatetimeTZValue) = v.value
get_value(v::DurationValue)   = v.value
get_value(v::StructValue)     = v.fields
get_value(a::Attribute)       = get_value(a.value)
get_value(::Any)              = nothing

# ─── Display / show ───────────────────────────────────────────────────────────

function Base.show(io::IO, d::Decimal)
    frac = rstrip(lpad(string(d.fractional), 19, '0'), '0')
    isempty(frac) ? print(io, d.integer) : print(io, d.integer, ".", frac)
end

function Base.show(io::IO, d::TypeDBDuration)
    print(io, "P")
    years, months = divrem(Int(d.months), 12)
    years  != 0 && print(io, years,       "Y")
    months != 0 && print(io, months,      "M")
    d.days != 0  && print(io, Int(d.days), "D")
    if d.nanos != 0
        print(io, "T")
        ns = Int64(d.nanos)
        h, ns = divrem(ns, 3_600_000_000_000)
        m, ns = divrem(ns, 60_000_000_000)
        s, ns = divrem(ns, 1_000_000_000)
        h != 0 && print(io, h, "H")
        m != 0 && print(io, m, "M")
        if s != 0 || ns != 0
            frac = rstrip(lpad(string(ns), 9, '0'), '0')
            isempty(frac) ? print(io, s, "S") : print(io, s, ".", frac, "S")
        end
    end
end

function Base.show(io::IO, tz::FixedTimeZone)
    h, rem = divrem(abs(tz.offset_seconds), 3600)
    m      = div(rem, 60)
    sign   = tz.offset_seconds >= 0 ? "+" : "-"
    print(io, "UTC", sign, lpad(h, 2, '0'), ":", lpad(m, 2, '0'))
end
Base.show(io::IO, tz::IANATimeZone) = print(io, tz.name)

Base.show(io::IO, dtz::DatetimeTZ) =
    print(io, dtz.datetime, "[", dtz.timezone, "]")

Base.show(io::IO, v::BooleanValue)    = print(io, v.value)
Base.show(io::IO, v::IntegerValue)    = print(io, v.value)
Base.show(io::IO, v::DoubleValue)     = print(io, v.value)
Base.show(io::IO, v::DecimalValue)    = print(io, v.value)
Base.show(io::IO, v::StringValue)     = print(io, '"', v.value, '"')
Base.show(io::IO, v::DateValue)       = print(io, v.value)
Base.show(io::IO, v::DatetimeValue)   = print(io, v.value)
Base.show(io::IO, v::DatetimeTZValue) = print(io, v.value)
Base.show(io::IO, v::DurationValue)   = print(io, v.value)
Base.show(io::IO, v::StructValue)     = print(io, "Struct(", v.fields, ")")

Base.show(io::IO, t::EntityType)    = print(io, "EntityType(", t.label, ")")
Base.show(io::IO, t::RelationType)  = print(io, "RelationType(", t.label, ")")
Base.show(io::IO, t::AttributeType) = print(io,
    "AttributeType(", t.label, isnothing(t.value_type) ? "" : ": " * t.value_type, ")")
Base.show(io::IO, t::RoleType)      = print(io, "RoleType(", t.label, ")")

Base.show(io::IO, e::Entity)   = print(io, "Entity(",
    e.iid, isnothing(e.type_) ? "" : " isa " * e.type_.label, ")")
Base.show(io::IO, r::Relation) = print(io, "Relation(",
    r.iid, isnothing(r.type_) ? "" : " isa " * r.type_.label, ")")
Base.show(io::IO, a::Attribute) = print(io, "Attribute(",
    a.value, isnothing(a.type_) ? "" : " isa " * a.type_.label, ")")

"""    concept_to_string(c) -> String — human-readable representation."""
concept_to_string(c::Concept) = sprint(show, c)

# ─── Equality and hashing ─────────────────────────────────────────────────────

Base.:(==)(a::EntityType,    b::EntityType)    = a.label == b.label
Base.:(==)(a::RelationType,  b::RelationType)  = a.label == b.label
Base.:(==)(a::AttributeType, b::AttributeType) = a.label == b.label && a.value_type == b.value_type
Base.:(==)(a::RoleType,      b::RoleType)      = a.label == b.label
Base.:(==)(a::Entity,        b::Entity)        = a.iid   == b.iid
Base.:(==)(a::Relation,      b::Relation)      = a.iid   == b.iid
Base.:(==)(a::Attribute,     b::Attribute)     = a.iid   == b.iid
Base.:(==)(a::Decimal,       b::Decimal)       = a.integer == b.integer && a.fractional == b.fractional
Base.:(==)(a::TypeDBDuration, b::TypeDBDuration) =
    a.months == b.months && a.days == b.days && a.nanos == b.nanos
Base.:(==)(a::DatetimeTZ,    b::DatetimeTZ)    = a.datetime == b.datetime && a.timezone == b.timezone
Base.:(==)(a::IANATimeZone,  b::IANATimeZone)  = a.name == b.name
Base.:(==)(a::FixedTimeZone, b::FixedTimeZone) = a.offset_seconds == b.offset_seconds

Base.hash(t::EntityType,    h::UInt) = hash(t.label, hash(:EntityType, h))
Base.hash(t::RelationType,  h::UInt) = hash(t.label, hash(:RelationType, h))
Base.hash(t::AttributeType, h::UInt) = hash(t.label, hash(t.value_type, hash(:AttributeType, h)))
Base.hash(t::RoleType,      h::UInt) = hash(t.label, hash(:RoleType, h))
Base.hash(e::Entity,        h::UInt) = hash(e.iid, hash(:Entity, h))
Base.hash(r::Relation,      h::UInt) = hash(r.iid, hash(:Relation, h))
Base.hash(a::Attribute,     h::UInt) = hash(a.iid, hash(:Attribute, h))
Base.hash(d::Decimal,       h::UInt) = hash(d.integer, hash(d.fractional, h))
Base.hash(d::TypeDBDuration, h::UInt) = hash(d.months, hash(d.days, hash(d.nanos, h)))
Base.hash(d::DatetimeTZ,    h::UInt) = hash(d.datetime, hash(d.timezone, h))
Base.hash(t::IANATimeZone,  h::UInt) = hash(t.name, hash(:IANA, h))
Base.hash(t::FixedTimeZone, h::UInt) = hash(t.offset_seconds, hash(:Fixed, h))

# ─── ConceptRow ───────────────────────────────────────────────────────────────

"""
    ConceptRow

A single row of query results mapping variable names to [`Concept`](@ref) values.

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

Retrieve and materialise the concept in the named column.
Throws if the column does not exist.
"""
function get_concept(row::ConceptRow, col::AbstractString)::Concept
    h = GC.@preserve col begin
        FFI.concept_row_get(
            row.handle,
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, col)))
    end
    check_and_throw()
    h == C_NULL && error("Column \"$col\" not found in ConceptRow")
    materialize(h)
end

"""
    get_concept(row, index::Integer) -> Concept

Retrieve and materialise the concept at the given zero-based column index.
"""
function get_concept(row::ConceptRow, idx::Integer)::Concept
    h = FFI.concept_row_get_index(row.handle, Csize_t(idx))
    check_and_throw()
    h == C_NULL && error("Column index $idx out of range")
    materialize(h)
end

"""
    column_names(row) -> Vector{String}

Return the variable names for all columns in this row.
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
            push!(names, typedb_owned_string(cstr))
        end
    finally
        FFI.string_iterator_drop(iter)
    end
    names
end

function Base.show(io::IO, row::ConceptRow)
    cstr = FFI.concept_row_to_string(row.handle)
    print(io, "ConceptRow(", typedb_owned_string(cstr), ")")
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
Base.eltype(::Type{ConceptRowIterator})       = ConceptRow

# ─── ConceptIterator ─────────────────────────────────────────────────────────

"""
    ConceptIterator

A Julia iterator that yields materialised [`Concept`](@ref) values from a
C-level concept iterator (e.g. from `concept_row_get_concepts`).
"""
mutable struct ConceptIterator
    handle::ConceptIterHandle
    _done::Bool

    function ConceptIterator(handle::ConceptIterHandle)
        handle == C_NULL && error("ConceptIterator handle is NULL")
        obj = new(handle, false)
        finalizer(obj) do it
            it._done || (it._done = true; FFI.concept_iterator_drop(it.handle))
        end
        obj
    end
end

function Base.iterate(iter::ConceptIterator, ::Nothing=nothing)
    iter._done && return nothing
    h = FFI.concept_iterator_next(iter.handle)
    check_and_throw()
    if h == C_NULL
        iter._done = true
        return nothing
    end
    return (materialize(h), nothing)
end

Base.IteratorSize(::Type{ConceptIterator}) = Base.SizeUnknown()
Base.eltype(::Type{ConceptIterator})       = Concept

"""
    concepts(row) -> ConceptIterator

Iterate over all concept values in a [`ConceptRow`](@ref) in column order.
"""
function concepts(row::ConceptRow)::ConceptIterator
    h = FFI.concept_row_get_concepts(row.handle)
    check_and_throw()
    ConceptIterator(h)
end

# ─── DocumentIterator (fetch queries returning JSON strings) ──────────────────

"""
    DocumentIterator

A Julia iterator over JSON document strings returned by a `fetch` query.
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
Base.eltype(::Type{DocumentIterator})       = String

# ─── QueryAnswer ─────────────────────────────────────────────────────────────

"""
    QueryAnswer

The result of a TypeDB query.  Depending on the query type it holds:
- A row stream (`match … select`, `insert`, …): consume with [`rows`](@ref)
- A document stream (`fetch`): consume with [`documents`](@ref)
- A success flag (schema/write queries): check with [`is_ok`](@ref)
"""
mutable struct QueryAnswer
    handle::QueryAnswerHandle
    _consumed::Bool
    _is_ok::Bool
    _is_row_stream::Bool
    _is_document_stream::Bool

    function QueryAnswer(handle::QueryAnswerHandle)
        handle == C_NULL && error("QueryAnswer handle is NULL")
        # Cache type flags at construction time.
        # After rows()/documents() consumes the handle via take_ownership in Rust,
        # the original pointer is freed; reading it afterwards is undefined behaviour.
        # Caching here makes is_ok/is_row_stream/is_document_stream safe post-consume.
        _ok   = FFI.query_answer_is_ok(handle)
        _rows = FFI.query_answer_is_concept_row_stream(handle)
        _docs = FFI.query_answer_is_concept_document_stream(handle)
        obj = new(handle, false, _ok, _rows, _docs)
        finalizer(obj) do qa
            qa._consumed || (qa._consumed = true; FFI.query_answer_drop(qa.handle))
        end
        obj
    end
end

"""    is_ok(answer::QueryAnswer) -> Bool"""
is_ok(qa::QueryAnswer) = qa._is_ok

"""    is_row_stream(answer::QueryAnswer) -> Bool"""
is_row_stream(qa::QueryAnswer) = qa._is_row_stream

"""    is_document_stream(answer::QueryAnswer) -> Bool"""
is_document_stream(qa::QueryAnswer) = qa._is_document_stream

"""
    rows(answer::QueryAnswer) -> ConceptRowIterator

Convert a row-stream answer into a Julia iterator.  Can only be called once.
"""
function rows(qa::QueryAnswer)::ConceptRowIterator
    qa._consumed && error("QueryAnswer already consumed")
    is_row_stream(qa) || error("QueryAnswer is not a rows stream")
    qa._consumed = true
    h = FFI.query_answer_into_rows(qa.handle)
    check_and_throw()
    ConceptRowIterator(h)
end

"""
    documents(answer::QueryAnswer) -> DocumentIterator

Convert a document-stream answer into a Julia iterator.  Can only be called once.
"""
function documents(qa::QueryAnswer)::DocumentIterator
    qa._consumed && error("QueryAnswer already consumed")
    qa._consumed = true
    h = FFI.query_answer_into_documents(qa.handle)
    check_and_throw()
    DocumentIterator(h)
end
