# ─── Opaque handle types (all are Ptr{Cvoid} at the FFI boundary) ──────────────

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

# ─── TransactionType enum ───────────────────────────────────────────────────────
# Mirrors the Rust `TransactionType` enum in the C driver.
module TransactionType
    "Read-only transaction"
    const READ   = Int32(0)
    "Read-write transaction"
    const WRITE  = Int32(1)
    "Schema modification transaction"
    const SCHEMA = Int32(2)
end

# ─── QueryType enum (returned by query_answer_get_query_type) ───────────────────
module QueryType
    const READ     = Int32(0)
    const WRITE    = Int32(1)
    const SCHEMA   = Int32(2)
end
