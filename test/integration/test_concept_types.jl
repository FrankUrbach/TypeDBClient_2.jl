using Test
using TypeDBClient
using Dates

# Integration tests for the rich Concept type system.
# Requires a running TypeDB 3.x server (TYPEDB_TEST_ADDRESS must be set).

const _CT_DB = "typedbclient_concept_test_$(getpid())"

# ─── Schema & data setup ──────────────────────────────────────────────────────

const _SCHEMA = """
define
  entity person,
    owns name,
    owns age,
    owns height,
    owns active,
    owns birthdate,
    owns registered_at;

  entity company,
    owns name;

  relation employment,
    relates employee,
    relates employer;

  person plays employment:employee;
  company plays employment:employer;

  attribute name,          value string;
  attribute age,           value integer;
  attribute height,        value double;
  attribute active,        value boolean;
  attribute birthdate,     value date;
  attribute registered_at, value datetime;
"""

const _INSERT = raw"""
insert
  $p isa person,
    has name "Alice",
    has age 30,
    has height 1.72,
    has active true,
    has birthdate 1994-01-15,
    has registered_at 2024-06-15T10:30:00;
  $c isa company,
    has name "Acme";
  (employee: $p, employer: $c) isa employment;
"""

function setup_concept_db(driver)
    contains_database(driver, _CT_DB) && delete_database(driver, _CT_DB)
    create_database(driver, _CT_DB)
    transaction(driver, _CT_DB, TransactionType.SCHEMA) do tx
        query(tx, _SCHEMA)
    end
    transaction(driver, _CT_DB, TransactionType.WRITE) do tx
        query(tx, _INSERT)
    end
end

function teardown_concept_db(driver)
    contains_database(driver, _CT_DB) && delete_database(driver, _CT_DB)
end

# ─── Helper: collect first row of a query ─────────────────────────────────────

function first_row(tx, typeql)
    ans = query(tx, typeql)
    @assert is_row_stream(ans)
    first(rows(ans))
end

# ─── Entity instance ──────────────────────────────────────────────────────────

@testset "Concept – Entity materialisation" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person;")
                c = get_concept(row, "p")

                # Union type check
                @test c isa Concept
                @test c isa TypeDBInstance
                @test c isa Entity

                # Kind predicates
                @test  is_entity(c)
                @test  is_instance(c)
                @test !is_relation(c)
                @test !is_attribute(c)
                @test !is_type(c)
                @test !is_value(c)

                # IID
                @test try_get_iid(c) isa String
                @test !isempty(try_get_iid(c))

                # Type info
                @test c.type_ isa EntityType
                @test c.type_.label == "person"
                @test get_label(c)       == "person"
                @test try_get_label(c)   == "person"
                @test try_get_value_type(c) === nothing
                @test get_value(c)          === nothing

                # EntityType checks
                et = c.type_
                @test is_entity_type(et)
                @test is_type(et)
                @test !is_relation_type(et)
                @test get_label(et) == "person"
                @test try_get_iid(et) === nothing
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Relation instance ────────────────────────────────────────────────────────

@testset "Concept – Relation materialisation" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $e isa employment;")
                c = get_concept(row, "e")

                @test c isa Concept
                @test c isa Relation
                @test  is_relation(c)
                @test  is_instance(c)
                @test !is_entity(c)
                @test !is_value(c)

                @test try_get_iid(c) isa String
                @test !isempty(try_get_iid(c))

                @test c.type_ isa RelationType
                @test c.type_.label == "employment"
                @test get_label(c) == "employment"

                # RelationType checks
                rt = c.type_
                @test is_relation_type(rt)
                @test !is_entity_type(rt)
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Attribute – string value ─────────────────────────────────────────────────

@testset "Concept – Attribute string value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has name $n;")
                c = get_concept(row, "n")

                @test c isa Concept
                @test c isa Attribute
                @test  is_attribute(c)
                @test  is_instance(c)
                @test !is_entity(c)
                @test !is_value(c)

                # value-kind predicates forwarded through Attribute
                @test !is_boolean(c)
                @test !is_integer(c)

                # Type info
                @test c.type_ isa AttributeType
                @test c.type_.label      == "name"
                @test c.type_.value_type == "string"
                @test get_label(c)          == "name"
                @test try_get_value_type(c) == "string"

                # Value extraction
                @test c.value isa StringValue
                @test get_value(c) == "Alice"
                @test get_value(c) isa String
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Attribute – integer value ────────────────────────────────────────────────

@testset "Concept – Attribute integer value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has age $a;")
                c = get_concept(row, "a")

                @test c isa Attribute
                @test is_integer(c)
                @test !is_double(c)
                @test c.value isa IntegerValue
                @test c.type_.value_type == "integer"
                @test get_value(c) == 30
                @test get_value(c) isa Int64
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Attribute – double value ─────────────────────────────────────────────────

@testset "Concept – Attribute double value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has height $h;")
                c = get_concept(row, "h")

                @test c isa Attribute
                @test is_double(c)
                @test !is_integer(c)
                @test c.value isa DoubleValue
                @test c.type_.value_type == "double"
                @test get_value(c) ≈ 1.72
                @test get_value(c) isa Float64
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Attribute – boolean value ────────────────────────────────────────────────

@testset "Concept – Attribute boolean value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has active $a;")
                c = get_concept(row, "a")

                @test c isa Attribute
                @test is_boolean(c)
                @test !is_integer(c)
                @test c.value isa BooleanValue
                @test c.type_.value_type == "boolean"
                @test get_value(c) === true
                @test get_value(c) isa Bool
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Attribute – date value ───────────────────────────────────────────────────

@testset "Concept – Attribute date value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has birthdate $d;")
                c = get_concept(row, "d")

                @test c isa Attribute
                @test is_date(c)
                @test !is_datetime(c)
                @test c.value isa DateValue
                @test c.type_.value_type == "date"
                @test get_value(c) isa Date
                @test get_value(c) == Date(1994, 1, 15)
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Attribute – datetime value ───────────────────────────────────────────────

@testset "Concept – Attribute datetime value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has registered_at $dt;")
                c = get_concept(row, "dt")

                @test c isa Attribute
                @test is_datetime(c)
                @test !is_date(c)
                @test !is_datetime_tz(c)
                @test c.value isa DatetimeValue
                @test c.type_.value_type == "datetime"
                @test get_value(c) isa DateTime
                @test get_value(c) == DateTime(2024, 6, 15, 10, 30, 0)
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Schema type access via instance.type_ ────────────────────────────────────

@testset "Concept – AttributeType fields from Attribute.type_" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has age $a;")
                a   = get_concept(row, "a")
                at  = a.type_

                @test at isa AttributeType
                @test at isa TypeDBType
                @test at isa Concept
                @test is_attribute_type(at)
                @test is_type(at)
                @test !is_entity_type(at)
                @test at.label      == "age"
                @test at.value_type == "integer"
                @test get_label(at)          == "age"
                @test try_get_value_type(at) == "integer"
                @test try_get_iid(at)        === nothing
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Column access by index ───────────────────────────────────────────────────

@testset "Concept – get_concept by column index" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has name $n;")
                cols = column_names(row)
                @test length(cols) >= 2

                # Access each column by index and verify it's a Concept
                for idx in 0:(length(cols)-1)
                    c = get_concept(row, idx)
                    @test c isa Concept
                end

                # Results by name and by index should agree
                p_by_name  = get_concept(row, "p")
                p_idx      = findfirst(==("p"), cols) - 1   # 0-based
                p_by_index = get_concept(row, p_idx)
                @test is_entity(p_by_name)
                @test is_entity(p_by_index)
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── concepts(row) iterator ───────────────────────────────────────────────────

@testset "Concept – concepts(row) iterator" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row  = first_row(tx, raw"match $p isa person, has name $n;")
                cols = column_names(row)

                # Collect via concepts() iterator
                clist = collect(concepts(row))
                @test length(clist) == length(cols)
                for c in clist
                    @test c isa Concept
                end
                # At least one entity and one attribute in this row
                @test any(is_entity,    clist)
                @test any(is_attribute, clist)
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Multiple rows, multiple value types ─────────────────────────────────────

@testset "Concept – multiple rows, collect names" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.WRITE) do tx
                query(tx, raw"""insert $p isa person, has name "Bob", has age 25;""")
            end
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                ans  = query(tx, raw"match $p isa person, has name $n;")
                names = String[]
                for row in rows(ans)
                    c = get_concept(row, "n")
                    @test c isa Attribute
                    @test is_attribute(c)
                    val = get_value(c)
                    @test val isa String
                    push!(names, val)
                end
                sort!(names)
                @test names == ["Alice", "Bob"]
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Rich type structure round-trip ──────────────────────────────────────────

@testset "Concept – rich type structure round-trip" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                ans = query(tx, raw"match $p isa person, has name $n, has age $a;")
                for row in rows(ans)
                    p = get_concept(row, "p")
                    n = get_concept(row, "n")
                    a = get_concept(row, "a")

                    # p is an Entity
                    @test p isa Entity
                    @test p.type_ isa EntityType
                    @test p.type_.label == "person"
                    @test !isempty(p.iid)

                    # n is a string Attribute
                    @test n isa Attribute
                    @test n.value isa StringValue
                    @test n.type_.label == "name"
                    @test get_value(n) isa String

                    # a is an integer Attribute
                    @test a isa Attribute
                    @test a.value isa IntegerValue
                    @test a.type_.label == "age"
                    @test get_value(a) isa Int64

                    # concept_to_string produces non-empty strings
                    @test !isempty(concept_to_string(p))
                    @test !isempty(concept_to_string(n))
                    @test !isempty(concept_to_string(a))
                end
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Value concepts from raw value queries ────────────────────────────────────

@testset "Concept – raw value from expression" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                # TypeDB 3.x: values can appear directly in select/fetch
                row = first_row(tx, raw"match $p isa person, has age $a; select $a;")
                a = get_concept(row, "a")
                @test a isa Concept
                @test is_attribute(a)
                @test get_value(a) isa Int64
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Decimal value type ───────────────────────────────────────────────────────
# Requires a TypeDB 3.x server and `decimal` attribute type support.

@testset "Concept – Attribute decimal value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            # Add decimal attribute to schema
            transaction(driver, _CT_DB, TransactionType.SCHEMA) do tx
                query(tx, "define attribute balance, value decimal; person owns balance;")
            end
            transaction(driver, _CT_DB, TransactionType.WRITE) do tx
                query(tx, raw"match $p isa person, has name \"Alice\"; insert $p has balance 99.50dec;")
            end
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has balance $b;")
                c = get_concept(row, "b")

                @test c isa Attribute
                @test is_decimal(c)
                @test !is_double(c)
                @test c.value isa DecimalValue
                @test c.type_.value_type == "decimal"
                d = get_value(c)
                @test d isa Decimal
                @test d.integer == 99
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Duration value type ──────────────────────────────────────────────────────

@testset "Concept – Attribute duration value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.SCHEMA) do tx
                query(tx, "define attribute tenure, value duration; person owns tenure;")
            end
            transaction(driver, _CT_DB, TransactionType.WRITE) do tx
                query(tx, raw"match $p isa person, has name \"Alice\"; insert $p has tenure P1Y6M;")
            end
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has tenure $t;")
                c = get_concept(row, "t")

                @test c isa Attribute
                @test is_duration(c)
                @test c.value isa DurationValue
                @test c.type_.value_type == "duration"
                dur = get_value(c)
                @test dur isa TypeDBDuration
                @test dur.months == 18   # 1Y6M = 18 months
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Datetime-TZ value type ───────────────────────────────────────────────────

@testset "Concept – Attribute datetime-tz value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        try
            transaction(driver, _CT_DB, TransactionType.SCHEMA) do tx
                query(tx, "define attribute scheduled, value datetime-tz; person owns scheduled;")
            end
            transaction(driver, _CT_DB, TransactionType.WRITE) do tx
                query(tx, raw"match $p isa person, has name \"Alice\"; insert $p has scheduled 2024-06-15T10:30:00+0200;")
            end
            transaction(driver, _CT_DB, TransactionType.READ) do tx
                row = first_row(tx, raw"match $p isa person, has scheduled $s;")
                c = get_concept(row, "s")

                @test c isa Attribute
                @test is_datetime_tz(c)
                @test !is_datetime(c)
                @test c.value isa DatetimeTZValue
                @test c.type_.value_type == "datetime-tz"
                dtz = get_value(c)
                @test dtz isa DatetimeTZ
                @test dtz.timezone isa TimeZoneSpec
                @test dtz.datetime isa DateTime
            end
        finally
            teardown_concept_db(driver)
        end
    end
end

# ─── Struct value type ────────────────────────────────────────────────────────

# Struct value types require TypeDB 3.x with struct support.
# The test is wrapped in a try/catch so it produces a clear skip message
# rather than an error when the server version or TypeQL syntax differs.
@testset "Concept – Attribute struct value" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_concept_db(driver)
        struct_ok = false
        try
            # Define a struct type and an attribute that uses it.
            transaction(driver, _CT_DB, TransactionType.SCHEMA) do tx
                query(tx, raw"""
                    define
                      struct Address { street: string, city: string };
                      attribute address, value Address;
                      person owns address;
                """)
            end
            struct_ok = true
        catch e
            @info "Skipping struct value test (struct type not supported or syntax differs)" exception=e
        end

        if struct_ok
            try
                transaction(driver, _CT_DB, TransactionType.WRITE) do tx
                    query(tx, raw"""
                        match $p isa person, has name "Alice";
                        insert $p has address Address{street: "Main St", city: "Springfield"};
                    """)
                end
                transaction(driver, _CT_DB, TransactionType.READ) do tx
                    row = first_row(tx, raw"match $p isa person, has address $a;")
                    c = get_concept(row, "a")

                    @test c isa Attribute
                    @test is_struct_value(c)
                    @test c.value isa StructValue
                    @test c.type_.value_type == "Address"
                    fields = get_value(c)
                    @test fields isa Dict
                    @test fields["street"] isa StringValue
                    @test get_value(fields["street"]) == "Main St"
                    @test fields["city"] isa StringValue
                    @test get_value(fields["city"]) == "Springfield"
                end
            finally
                teardown_concept_db(driver)
            end
        else
            teardown_concept_db(driver)
            @test_skip "struct attribute type not available on this server"
        end
    end
end
