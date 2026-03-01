using Test
using TypeDBClient

const _TEST_DB = "typedbclient_test_$(getpid())"

@testset "Databases – lifecycle" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        contains_database(driver, _TEST_DB) && delete_database(driver, _TEST_DB)

        @test !contains_database(driver, _TEST_DB)

        create_database(driver, _TEST_DB)
        @test contains_database(driver, _TEST_DB)

        db = get_database(driver, _TEST_DB)
        @test db isa Database
        @test database_name(db) == _TEST_DB

        dbs = list_databases(driver)
        @test any(d -> database_name(d) == _TEST_DB, dbs)

        delete_database(driver, _TEST_DB)
        @test !contains_database(driver, _TEST_DB)
    end
end

@testset "Databases – get_database non-existent throws" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        @test_throws TypeDBError get_database(driver, "does_not_exist_$(getpid())")
    end
end

@testset "Databases – schema" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        contains_database(driver, _TEST_DB) || create_database(driver, _TEST_DB)

        db = get_database(driver, _TEST_DB)
        schema = database_schema(db)
        @test schema isa String

        type_schema = database_type_schema(db)
        @test type_schema isa String

        delete_database(driver, _TEST_DB)
    end
end
