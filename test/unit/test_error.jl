using Test

# Unit tests for error.jl
# These tests mock the FFI layer to avoid needing a real library.

@testset "TypeDBError" begin
    e = TypeDBClient.TypeDBError("TST001", "test error message")
    @test e isa Exception
    @test e.code    == "TST001"
    @test e.message == "test error message"

    # showerror
    buf = IOBuffer()
    showerror(buf, e)
    s = String(take!(buf))
    @test occursin("TST001", s)
    @test occursin("test error message", s)
end

@testset "TypeDBError – default fields" begin
    e = TypeDBClient.TypeDBError("", "")
    @test e.code    == ""
    @test e.message == ""
end
