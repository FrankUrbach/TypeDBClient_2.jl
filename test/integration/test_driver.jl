using Test
using TypeDBClient

@testset "Driver – open / close" begin
    driver = TypeDBDriver(TEST_ADDRESS)
    @test driver isa TypeDBDriver
    @test isopen(driver)
    close(driver)
    @test driver._closed == true
end

@testset "Driver – do-block" begin
    opened = Ref(false)
    TypeDBDriver(TEST_ADDRESS) do driver
        opened[] = true
        @test isopen(driver)
    end
    @test opened[]
end

@testset "Driver – do-block closes on exception" begin
    local captured_driver = nothing
    try
        TypeDBDriver(TEST_ADDRESS) do driver
            captured_driver = driver
            error("intentional")
        end
    catch e
        @test e isa ErrorException
        @test e.msg == "intentional"
    end
    @test captured_driver !== nothing
    @test captured_driver._closed == true
end
