module TypeDBClient

# ─── Load deps (provides `libtypedb` and `check_deps`) ────────────────────────
const _DEPS_FILE = joinpath(@__DIR__, "..", "deps", "deps.jl")

if isfile(_DEPS_FILE)
    include(_DEPS_FILE)
else
    # Fallback: define a placeholder so the module at least compiles.
    # Users must run `Pkg.build("TypeDBClient")` to generate deps.jl.
    const libtypedb = ""
    function check_deps()
        error("deps/deps.jl not found. Run `Pkg.build(\"TypeDBClient\")` first.")
    end
end

# ─── Handle type aliases (Ptr{Cvoid} for every opaque C struct) ────────────────
const TypeDBDriverHandle          = Ptr{Cvoid}
const CredentialsHandle           = Ptr{Cvoid}
const DriverOptionsHandle         = Ptr{Cvoid}
const DatabaseHandle              = Ptr{Cvoid}
const DatabaseIterHandle          = Ptr{Cvoid}
const TransactionHandle           = Ptr{Cvoid}
const TransactionOptionsHandle    = Ptr{Cvoid}
const QueryOptionsHandle          = Ptr{Cvoid}
const VoidPromHandle              = Ptr{Cvoid}
const QueryAnswerPromHandle       = Ptr{Cvoid}
const QueryAnswerHandle           = Ptr{Cvoid}
const ConceptRowIterHandle        = Ptr{Cvoid}
const ConceptRowHandle            = Ptr{Cvoid}
const ConceptIterHandle           = Ptr{Cvoid}
const ConceptHandle               = Ptr{Cvoid}
const StringIterHandle            = Ptr{Cvoid}
const ErrorHandle                 = Ptr{Cvoid}

# ─── Load sub-files in dependency order ────────────────────────────────────────
include("ffi/types.jl")     # TransactionType, QueryType enums
include("ffi/functions.jl") # module FFI with all ccall wrappers
include("error.jl")         # TypeDBError, check_and_throw, @checkerr
include("strings.jl")       # typedb_string, typedb_owned_string
include("driver.jl")        # TypeDBDriver
include("database.jl")      # Database, DatabaseManager ops
include("transaction.jl")   # Transaction, transaction do-block
include("concept.jl")       # Concept, ConceptRow, QueryAnswer, iterators
include("query.jl")         # query()
include("exports.jl")       # public exports

# ─── Module initialiser ────────────────────────────────────────────────────────

"""
    TypeDBClient.init_logging()

Enable the TypeDB driver's internal logging.  Behaviour is controlled by the
environment variables `TYPEDB_DRIVER_LOG` and `TYPEDB_DRIVER_LOG_LEVEL`.
"""
function init_logging()
    FFI.init_logging()
end

function __init__()
    if isfile(_DEPS_FILE)
        try
            check_deps()
        catch err
            @warn "TypeDBClient: library check failed – $(err)\n" *
                  "Run `Pkg.build(\"TypeDBClient\")` to rebuild."
        end
    end
end

end # module TypeDBClient
