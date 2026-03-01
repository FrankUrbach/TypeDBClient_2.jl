using Test

# Unit tests for strings.jl
# typedb_string and typedb_owned_string work purely in Julia (no FFI needed).

@testset "typedb_string" begin
    # Normal string
    s = "hello"
    cstr = Base.unsafe_convert(Cstring, Base.cconvert(Cstring, s))
    result = GC.@preserve s TypeDBClient.typedb_string(cstr)
    @test result == "hello"

    # NULL pointer → empty string
    result_null = TypeDBClient.typedb_string(Cstring(C_NULL))
    @test result_null == ""
end

@testset "typedb_owned_string – NULL" begin
    # NULL pointer → empty string (no free called)
    result = TypeDBClient.typedb_owned_string(Cstring(C_NULL))
    @test result == ""
end
