# ─── String-ownership helpers ─────────────────────────────────────────────────
#
# The TypeDB C driver follows two distinct ownership conventions for strings:
#
# 1. Borrowed strings  – the C side retains ownership; we must NOT free the pointer.
#    Example: database_get_name() – the string belongs to the Database object.
#
# 2. Owned strings     – the C side allocates a new string; we are responsible
#    for calling string_free() after copying it into Julia.
#    Example: database_schema(), concept_get_string(), error_message().
#
# Use `typedb_string` for borrowed pointers and `typedb_owned_string` for owned
# pointers.  Both return a Julia `String` (or `""` for NULL pointers).

"""
    typedb_string(cstr::Cstring) -> String

Copy a *borrowed* C string into a Julia `String`.  The pointer is **not** freed.
"""
function typedb_string(cstr::Cstring)::String
    cstr == C_NULL && return ""
    unsafe_string(cstr)
end

"""
    typedb_owned_string(cstr::Cstring) -> String

Copy an *owned* C string into a Julia `String`, then free the C memory via
`string_free`.
"""
function typedb_owned_string(cstr::Cstring)::String
    cstr == C_NULL && return ""
    s = unsafe_string(cstr)
    FFI.string_free(cstr)
    s
end
