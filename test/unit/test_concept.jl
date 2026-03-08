using Test
using TypeDBClient
using Dates

# Unit tests for the rich Concept type system (concept.jl).
# No TypeDB server required — all tests exercise pure Julia logic.

# ─── FFI struct ABI sizes ─────────────────────────────────────────────────────
# These must match the C ABI exactly; wrong sizes → wrong reads / memory corruption.

@testset "FFI struct sizes match C ABI (x86-64)" begin
    @test sizeof(TypeDBClient.FFI.Decimal)             == 16   # { i64, u64 }
    @test sizeof(TypeDBClient.FFI.DatetimeInNanos)     == 16   # { i64, u32, _pad }
    @test sizeof(TypeDBClient.FFI.Duration)            == 16   # { u32, u32, u64 }
    @test sizeof(TypeDBClient.FFI.DatetimeAndTimeZone) == 32   # { DatetimeInNanos(16), *char(8), i32(4), bool(1), _pad(3) }
    @test sizeof(TypeDBClient.FFI.StringAndOptValue)   == 16   # { *char(8), *Concept(8) }
end

# ─── Decimal ──────────────────────────────────────────────────────────────────

@testset "Decimal – construction and fields" begin
    d = Decimal(42, 5_000_000_000_000_000_000)
    @test d.integer    == 42
    @test d.fractional == 5_000_000_000_000_000_000
end

@testset "Decimal – equality and hash" begin
    a = Decimal(10, 250_000_000_000_000_000)
    b = Decimal(10, 250_000_000_000_000_000)
    c = Decimal(10, 0)
    @test a == b
    @test a != c
    @test hash(a) == hash(b)
    @test hash(a) != hash(c)
end

@testset "Decimal – show" begin
    @test sprint(show, Decimal(42, 5_000_000_000_000_000_000)) == "42.5"
    @test sprint(show, Decimal(7,  0))                         == "7"
    @test sprint(show, Decimal(-1, 5_000_000_000_000_000_000)) == "-1.5"
    # 10^17 / 10^19 = 0.01
    @test sprint(show, Decimal(0, 100_000_000_000_000_000))    == "0.01"
    # full 19-digit fractional, no trailing zeros
    @test sprint(show, Decimal(1, 1))                          == "1.0000000000000000001"
end

# ─── TypeDBDuration ───────────────────────────────────────────────────────────

@testset "TypeDBDuration – construction and fields" begin
    d = TypeDBDuration(14, 10, 3_600_000_000_000)
    @test d.months == 14
    @test d.days   == 10
    @test d.nanos  == 3_600_000_000_000
end

@testset "TypeDBDuration – equality and hash" begin
    a = TypeDBDuration(1, 2, 3)
    b = TypeDBDuration(1, 2, 3)
    c = TypeDBDuration(0, 0, 0)
    @test a == b
    @test a != c
    @test hash(a) == hash(b)
    @test hash(a) != hash(c)
end

@testset "TypeDBDuration – show (ISO-8601)" begin
    # 14 months = 1Y2M, 10 days, 1 hour
    @test sprint(show, TypeDBDuration(14, 10, 3_600_000_000_000)) == "P1Y2M10DT1H"
    # zero duration
    @test sprint(show, TypeDBDuration(0, 0, 0))                   == "P"
    # 1.5 seconds
    @test sprint(show, TypeDBDuration(0, 0, 1_500_000_000))       == "PT1.5S"
    # 2 minutes 30 seconds
    @test sprint(show, TypeDBDuration(0, 0, 150_000_000_000))     == "PT2M30S"
    # 6 months only
    @test sprint(show, TypeDBDuration(6, 0, 0))                   == "P6M"
    # 3 days only
    @test sprint(show, TypeDBDuration(0, 3, 0))                   == "P3D"
    # mixed: 1Y, 5D, 2H30M
    nanos = 2 * 3_600_000_000_000 + 30 * 60_000_000_000
    @test sprint(show, TypeDBDuration(12, 5, nanos))              == "P1Y5DT2H30M"
end

# ─── TimeZoneSpec ─────────────────────────────────────────────────────────────

@testset "IANATimeZone" begin
    tz = IANATimeZone("Europe/Berlin")
    @test tz.name == "Europe/Berlin"
    @test tz isa TimeZoneSpec
    @test tz == IANATimeZone("Europe/Berlin")
    @test tz != IANATimeZone("America/New_York")
    @test sprint(show, tz) == "Europe/Berlin"
end

@testset "FixedTimeZone" begin
    @test FixedTimeZone(3600)  isa TimeZoneSpec
    @test FixedTimeZone(3600)  == FixedTimeZone(3600)
    @test FixedTimeZone(3600)  != FixedTimeZone(7200)
    @test sprint(show, FixedTimeZone(3600))  == "UTC+01:00"
    @test sprint(show, FixedTimeZone(-7200)) == "UTC-02:00"
    @test sprint(show, FixedTimeZone(5400))  == "UTC+01:30"
    @test sprint(show, FixedTimeZone(0))     == "UTC+00:00"
end

# ─── DatetimeTZ ───────────────────────────────────────────────────────────────

@testset "DatetimeTZ" begin
    dt  = DateTime(2024, 6, 15, 10, 30, 0)
    tz  = IANATimeZone("Europe/Berlin")
    dtz = DatetimeTZ(dt, tz)
    @test dtz.datetime == dt
    @test dtz.timezone == tz
    @test dtz == DatetimeTZ(dt, tz)
    @test dtz != DatetimeTZ(dt, FixedTimeZone(7200))
    s = sprint(show, dtz)
    @test occursin("2024-06-15", s)
    @test occursin("Europe/Berlin", s)
end

# ─── Concrete value types ─────────────────────────────────────────────────────

@testset "BooleanValue" begin
    v = BooleanValue(true)
    @test v isa TypeDBValue
    @test v isa Concept
    @test v.value === true
    @test is_value(v)
    @test is_boolean(v)
    @test !is_integer(v)
    @test get_value(v) === true
    @test try_get_value_type(v) == "boolean"
    @test get_label(v) == "boolean"
    @test sprint(show, v) == "true"
    @test BooleanValue(true) == BooleanValue(true)
    @test BooleanValue(true) != BooleanValue(false)
end

@testset "IntegerValue" begin
    v = IntegerValue(42)
    @test v isa TypeDBValue
    @test v isa Concept
    @test v.value == 42
    @test is_value(v)
    @test is_integer(v)
    @test !is_double(v)
    @test get_value(v) == 42
    @test get_value(v) isa Int64
    @test try_get_value_type(v) == "integer"
    @test get_label(v) == "integer"
    @test sprint(show, v) == "42"
end

@testset "DoubleValue" begin
    v = DoubleValue(3.14)
    @test v isa TypeDBValue
    @test is_double(v)
    @test !is_integer(v)
    @test get_value(v) ≈ 3.14
    @test get_value(v) isa Float64
    @test try_get_value_type(v) == "double"
end

@testset "DecimalValue" begin
    d = Decimal(99, 990_000_000_000_000_000)
    v = DecimalValue(d)
    @test v isa TypeDBValue
    @test is_decimal(v)
    @test !is_double(v)
    @test get_value(v) == d
    @test get_value(v) isa Decimal
    @test try_get_value_type(v) == "decimal"
    @test get_label(v) == "decimal"
end

@testset "StringValue" begin
    v = StringValue("hello")
    @test v isa TypeDBValue
    @test is_value(v)
    @test !is_boolean(v)
    @test get_value(v) == "hello"
    @test get_value(v) isa String
    @test try_get_value_type(v) == "string"
    @test get_label(v) == "string"
    @test sprint(show, v) == "\"hello\""
end

@testset "DateValue" begin
    d = Date(2024, 6, 15)
    v = DateValue(d)
    @test v isa TypeDBValue
    @test is_date(v)
    @test !is_datetime(v)
    @test get_value(v) == d
    @test get_value(v) isa Date
    @test try_get_value_type(v) == "date"
end

@testset "DatetimeValue" begin
    dt = DateTime(2024, 6, 15, 10, 30, 0)
    v  = DatetimeValue(dt)
    @test v isa TypeDBValue
    @test is_datetime(v)
    @test !is_date(v)
    @test !is_datetime_tz(v)
    @test get_value(v) == dt
    @test get_value(v) isa DateTime
    @test try_get_value_type(v) == "datetime"
end

@testset "DatetimeTZValue" begin
    dtz = DatetimeTZ(DateTime(2024, 1, 1), IANATimeZone("UTC"))
    v   = DatetimeTZValue(dtz)
    @test v isa TypeDBValue
    @test is_datetime_tz(v)
    @test !is_datetime(v)
    @test get_value(v) == dtz
    @test get_value(v) isa DatetimeTZ
    @test try_get_value_type(v) == "datetime-tz"
end

@testset "DurationValue" begin
    dur = TypeDBDuration(0, 0, 60_000_000_000)
    v   = DurationValue(dur)
    @test v isa TypeDBValue
    @test is_duration(v)
    @test !is_date(v)
    @test get_value(v) == dur
    @test get_value(v) isa TypeDBDuration
    @test try_get_value_type(v) == "duration"
    @test sprint(show, v) == "PT1M"
end

@testset "StructValue" begin
    fields = Dict{String, Union{Nothing, TypeDBValue}}(
        "name"  => StringValue("Alice"),
        "score" => IntegerValue(100),
        "tag"   => nothing,
    )
    v = StructValue(fields)
    @test v isa TypeDBValue
    @test is_struct_value(v)
    @test !is_string_value(v)
    @test get_value(v) isa Dict
    @test get_value(v)["name"] == StringValue("Alice")
    @test get_value(v)["tag"]  === nothing
    @test try_get_value_type(v) == "struct"
end

# ─── Schema types ─────────────────────────────────────────────────────────────

@testset "EntityType" begin
    t = EntityType("person")
    @test t isa TypeDBType
    @test t isa Concept
    @test t.label == "person"
    @test is_entity_type(t)
    @test is_type(t)
    @test !is_relation_type(t)
    @test !is_instance(t)
    @test !is_value(t)
    @test get_label(t) == "person"
    @test try_get_label(t) == "person"
    @test try_get_iid(t) === nothing
    @test try_get_value_type(t) === nothing
    @test get_value(t) === nothing
    @test sprint(show, t) == "EntityType(person)"
    @test EntityType("person") == EntityType("person")
    @test EntityType("person") != EntityType("company")
    @test hash(EntityType("a")) == hash(EntityType("a"))
end

@testset "RelationType" begin
    t = RelationType("friendship")
    @test t isa TypeDBType
    @test t isa Concept
    @test is_relation_type(t)
    @test is_type(t)
    @test !is_entity_type(t)
    @test get_label(t) == "friendship"
    @test sprint(show, t) == "RelationType(friendship)"
    @test RelationType("x") == RelationType("x")
    @test RelationType("x") != RelationType("y")
end

@testset "AttributeType – with value_type" begin
    t = AttributeType("age", "integer")
    @test t isa TypeDBType
    @test t isa Concept
    @test is_attribute_type(t)
    @test !is_entity_type(t)
    @test t.label == "age"
    @test t.value_type == "integer"
    @test get_label(t) == "age"
    @test try_get_value_type(t) == "integer"
    @test sprint(show, t) == "AttributeType(age: integer)"
    @test AttributeType("age", "integer") == AttributeType("age", "integer")
    @test AttributeType("age", "integer") != AttributeType("age", "string")
end

@testset "AttributeType – without value_type (abstract)" begin
    t = AttributeType("attr", nothing)
    @test t.value_type === nothing
    @test try_get_value_type(t) === nothing
    @test sprint(show, t) == "AttributeType(attr)"
end

@testset "RoleType" begin
    t = RoleType("employment:employee")
    @test t isa TypeDBType
    @test t isa Concept
    @test is_role_type(t)
    @test !is_entity_type(t)
    @test get_label(t) == "employment:employee"
    @test sprint(show, t) == "RoleType(employment:employee)"
    @test RoleType("a") == RoleType("a")
    @test RoleType("a") != RoleType("b")
end

# ─── Instance types ───────────────────────────────────────────────────────────

@testset "Entity – with type" begin
    e = Entity("0xabc123", EntityType("person"))
    @test e isa TypeDBInstance
    @test e isa Concept
    @test e.iid == "0xabc123"
    @test e.type_ == EntityType("person")
    @test is_entity(e)
    @test is_instance(e)
    @test !is_relation(e)
    @test !is_type(e)
    @test !is_value(e)
    @test try_get_iid(e) == "0xabc123"
    @test get_label(e) == "person"
    @test try_get_label(e) == "person"
    @test try_get_value_type(e) === nothing
    @test get_value(e) === nothing
    s = sprint(show, e)
    @test occursin("Entity", s)
    @test occursin("0xabc123", s)
    @test occursin("person", s)
    @test Entity("x", nothing) == Entity("x", nothing)
    @test Entity("x", nothing) != Entity("y", nothing)
    @test hash(Entity("x", nothing)) == hash(Entity("x", nothing))
end

@testset "Entity – without type (type info disabled)" begin
    e = Entity("0xabc", nothing)
    @test e.type_ === nothing
    @test get_label(e) == "unknown"
    @test try_get_label(e) === nothing
    @test sprint(show, e) == "Entity(0xabc)"
end

@testset "Relation – with type" begin
    r = Relation("0xdef456", RelationType("employment"))
    @test r isa TypeDBInstance
    @test r isa Concept
    @test is_relation(r)
    @test !is_entity(r)
    @test r.iid == "0xdef456"
    @test get_label(r) == "employment"
    @test try_get_iid(r) == "0xdef456"
    s = sprint(show, r)
    @test occursin("Relation", s)
    @test occursin("employment", s)
end

@testset "Attribute – string value" begin
    a = Attribute("0x111", StringValue("Alice"), AttributeType("name", "string"))
    @test a isa TypeDBInstance
    @test a isa Concept
    @test is_attribute(a)
    @test is_instance(a)
    @test !is_entity(a)
    @test !is_value(a)
    @test a.iid == "0x111"
    @test a.value == StringValue("Alice")
    @test a.type_ == AttributeType("name", "string")
    @test get_label(a) == "name"
    @test try_get_iid(a) == "0x111"
    @test try_get_value_type(a) == "string"
    # get_value delegates to the wrapped TypeDBValue
    @test get_value(a) == "Alice"
    @test get_value(a) isa String
    # value-kind predicates forward to a.value
    @test !is_boolean(a)
    @test !is_integer(a)
    s = sprint(show, a)
    @test occursin("Attribute", s)
    @test occursin("Alice", s)
    @test occursin("name", s)
    @test Attribute("x", IntegerValue(1), nothing) == Attribute("x", BooleanValue(true), nothing)  # equality by IID only
    @test Attribute("x", IntegerValue(1), nothing) != Attribute("y", IntegerValue(1), nothing)
end

@testset "Attribute – integer value, value-kind predicates forwarded" begin
    a = Attribute("0x222", IntegerValue(30), AttributeType("age", "integer"))
    @test is_integer(a)
    @test !is_string_value(a)
    @test get_value(a) == 30
    @test get_value(a) isa Int64
    @test try_get_value_type(a) == "integer"
end

@testset "Attribute – without type" begin
    a = Attribute("0x333", BooleanValue(false), nothing)
    @test a.type_ === nothing
    @test get_label(a) == "unknown"
    @test try_get_label(a) === nothing
    @test try_get_value_type(a) === nothing
end

# ─── Concept union ────────────────────────────────────────────────────────────

@testset "Concept union – isa checks" begin
    concepts = [
        EntityType("x"), RelationType("x"), AttributeType("x", nothing), RoleType("x"),
        Entity("i", nothing), Relation("i", nothing),
        Attribute("i", IntegerValue(0), nothing),
        BooleanValue(true), IntegerValue(0), DoubleValue(0.0), DecimalValue(Decimal(0,0)),
        StringValue(""), DateValue(Date(2000,1,1)), DatetimeValue(DateTime(2000,1,1)),
        DatetimeTZValue(DatetimeTZ(DateTime(2000,1,1), FixedTimeZone(0))),
        DurationValue(TypeDBDuration(0,0,0)),
        StructValue(Dict{String,Union{Nothing,TypeDBValue}}()),
    ]
    for c in concepts
        @test c isa Concept
    end
end

# ─── Kind predicates – mutual exclusivity ────────────────────────────────────

@testset "Kind predicates are mutually exclusive" begin
    e  = Entity("i", nothing)
    r  = Relation("i", nothing)
    a  = Attribute("i", IntegerValue(0), nothing)
    et = EntityType("x")
    v  = IntegerValue(42)

    @test  is_entity(e);    @test !is_relation(e);   @test !is_attribute(e)
    @test !is_entity(r);    @test  is_relation(r);   @test !is_attribute(r)
    @test !is_entity(a);    @test !is_relation(a);   @test  is_attribute(a)
    @test !is_entity(et);   @test !is_relation(et);  @test !is_attribute(et)
    @test !is_entity(v);    @test !is_relation(v);   @test !is_attribute(v)

    @test  is_type(et);     @test !is_type(e);       @test !is_type(v)
    @test  is_instance(e);  @test !is_instance(et);  @test !is_instance(v)
    @test  is_value(v);     @test !is_value(e);      @test !is_value(et)
end

# ─── concept_to_string ────────────────────────────────────────────────────────

@testset "concept_to_string" begin
    @test concept_to_string(EntityType("person")) == "EntityType(person)"
    @test concept_to_string(IntegerValue(7))       == "7"
    s = concept_to_string(Entity("0xabc", EntityType("person")))
    @test occursin("Entity",  s)
    @test occursin("person",  s)
    @test occursin("0xabc",   s)
end

# ─── get_value fallback ───────────────────────────────────────────────────────

@testset "get_value returns nothing for types and non-value instances" begin
    @test get_value(EntityType("x"))          === nothing
    @test get_value(RelationType("x"))        === nothing
    @test get_value(Entity("i", nothing))     === nothing
    @test get_value(Relation("i", nothing))   === nothing
end
