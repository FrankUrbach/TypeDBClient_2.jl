using Test
using TypeDBClient

const _Q_DB = "typedbclient_query_test_$(getpid())"

function setup_query_db(driver)
    contains_database(driver, _Q_DB) && delete_database(driver, _Q_DB)
    create_database(driver, _Q_DB)

    # TypeDB 3.x schema syntax
    transaction(driver, _Q_DB, TransactionType.SCHEMA) do tx
        query(tx, "define entity person, owns name, owns age; attribute name, value string; attribute age, value integer;")
    end

    transaction(driver, _Q_DB, TransactionType.WRITE) do tx
        query(tx, "insert \$p1 isa person, has name \"Alice\", has age 30; \$p2 isa person, has name \"Bob\", has age 25;")
    end
end

function teardown_query_db(driver)
    contains_database(driver, _Q_DB) && delete_database(driver, _Q_DB)
end

@testset "Query – match returns rows" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_query_db(driver)
        try
            transaction(driver, _Q_DB, TransactionType.READ) do tx
                ans = query(tx, "match \$p isa person;")
                @test is_row_stream(ans)
                result_rows = collect(rows(ans))
                @test length(result_rows) == 2
            end
        finally
            teardown_query_db(driver)
        end
    end
end

@testset "Query – get string attribute" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_query_db(driver)
        try
            transaction(driver, _Q_DB, TransactionType.READ) do tx
                ans = query(tx, "match \$p isa person, has name \$n;")
                names = String[]
                for row in rows(ans)
                    c = get_concept(row, "n")
                    @test c isa Concept
                    val = get_value(c)
                    @test val isa String
                    push!(names, val)
                end
                sort!(names)
                @test names == ["Alice", "Bob"]
            end
        finally
            teardown_query_db(driver)
        end
    end
end

@testset "Query – get integer attribute" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_query_db(driver)
        try
            transaction(driver, _Q_DB, TransactionType.READ) do tx
                ans = query(tx, "match \$p isa person, has age \$a;")
                ages = Int64[]
                for row in rows(ans)
                    c = get_concept(row, "a")
                    v = get_value(c)
                    @test v isa Int64
                    push!(ages, v)
                end
                sort!(ages)
                @test ages == [25, 30]
            end
        finally
            teardown_query_db(driver)
        end
    end
end

@testset "Query – column_names" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_query_db(driver)
        try
            transaction(driver, _Q_DB, TransactionType.READ) do tx
                ans = query(tx, "match \$p isa person;")
                result_rows = collect(rows(ans))
                @test !isempty(result_rows)
                cols = column_names(result_rows[1])
                @test "p" in cols
            end
        finally
            teardown_query_db(driver)
        end
    end
end

@testset "Query – fetch returns document stream" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_query_db(driver)
        try
            transaction(driver, _Q_DB, TransactionType.READ) do tx
                # TypeDB 3.x fetch syntax uses curly braces
                ans = query(tx, "match \$p isa person, has name \$n; fetch { \"name\": \$n };")
                @test is_document_stream(ans)
                docs = collect(documents(ans))
                @test length(docs) == 2
                for d in docs
                    @test d isa String
                    @test !isempty(d)
                end
            end
        finally
            teardown_query_db(driver)
        end
    end
end

@testset "Query – schema define / undefine" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_query_db(driver)
        try
            transaction(driver, _Q_DB, TransactionType.SCHEMA) do tx
                ans = query(tx, "define attribute email, value string;")
                @test is_ok(ans)
            end

            transaction(driver, _Q_DB, TransactionType.SCHEMA) do tx
                # TypeDB 3.x: undefine without type keyword
                ans = query(tx, "undefine email;")
                @test is_ok(ans)
            end
        finally
            teardown_query_db(driver)
        end
    end
end

@testset "Query – concept predicates" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_query_db(driver)
        try
            transaction(driver, _Q_DB, TransactionType.READ) do tx
                ans = query(tx, "match \$p isa person;")
                for row in rows(ans)
                    c = get_concept(row, "p")
                    @test is_entity(c)
                    @test !is_relation(c)
                    @test !is_value(c)
                end
            end
        finally
            teardown_query_db(driver)
        end
    end
end
