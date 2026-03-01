using Test
using TypeDBClient

const _TX_DB = "typedbclient_tx_test_$(getpid())"

function setup_tx_db(driver)
    contains_database(driver, _TX_DB) && delete_database(driver, _TX_DB)
    create_database(driver, _TX_DB)
end

function teardown_tx_db(driver)
    contains_database(driver, _TX_DB) && delete_database(driver, _TX_DB)
end

@testset "Transaction – open / close" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_tx_db(driver)
        try
            transaction(driver, _TX_DB, TransactionType.READ) do tx
                @test tx isa Transaction
                @test isopen(tx)
            end
        finally
            teardown_tx_db(driver)
        end
    end
end

@testset "Transaction – write auto-commits" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_tx_db(driver)
        try
            # Define schema (TypeDB 3.x syntax)
            transaction(driver, _TX_DB, TransactionType.SCHEMA) do tx
                query(tx, "define entity person, owns name; attribute name, value string;")
            end

            # Write data – should auto-commit
            transaction(driver, _TX_DB, TransactionType.WRITE) do tx
                query(tx, "insert \$p isa person, has name \"Testperson\";")
            end

            # Verify data persisted
            transaction(driver, _TX_DB, TransactionType.READ) do tx
                ans = query(tx, "match \$p isa person, has name \"Testperson\";")
                @test is_row_stream(ans)
                results = collect(rows(ans))
                @test length(results) == 1
            end
        finally
            teardown_tx_db(driver)
        end
    end
end

@testset "Transaction – rollback on exception" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_tx_db(driver)
        try
            transaction(driver, _TX_DB, TransactionType.SCHEMA) do tx
                query(tx, "define entity person, owns name; attribute name, value string;")
            end

            # Exception inside write block → rollback
            @test_throws ErrorException begin
                transaction(driver, _TX_DB, TransactionType.WRITE) do tx
                    query(tx, "insert \$p isa person, has name \"ShouldNotExist\";")
                    error("rollback me")
                end
            end

            # Verify nothing was written
            transaction(driver, _TX_DB, TransactionType.READ) do tx
                ans = query(tx, "match \$p isa person;")
                results = collect(rows(ans))
                @test isempty(results)
            end
        finally
            teardown_tx_db(driver)
        end
    end
end

@testset "Transaction – read does not auto-commit" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_tx_db(driver)
        try
            transaction(driver, _TX_DB, TransactionType.SCHEMA) do tx
                query(tx, "define entity person, owns name; attribute name, value string;")
            end

            transaction(driver, _TX_DB, TransactionType.READ) do tx
                @test !tx._committed
            end
        finally
            teardown_tx_db(driver)
        end
    end
end

@testset "Transaction – Database overload" begin
    TypeDBDriver(TEST_ADDRESS) do driver
        setup_tx_db(driver)
        try
            db = get_database(driver, _TX_DB)
            transaction(db, TransactionType.READ) do tx
                @test isopen(tx)
            end
        finally
            teardown_tx_db(driver)
        end
    end
end
