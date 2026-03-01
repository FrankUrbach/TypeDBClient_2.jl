# ─── TypeDB error type ─────────────────────────────────────────────────────────

"""
    TypeDBError(code, message)

Exception thrown whenever the TypeDB C driver signals an error.
`code` is the driver-level error code string; `message` is the human-readable
description.
"""
struct TypeDBError <: Exception
    code::String
    message::String
end

Base.showerror(io::IO, e::TypeDBError) =
    print(io, "TypeDBError [$(e.code)]: $(e.message)")

# ─── Error inspection helper ───────────────────────────────────────────────────

"""
    check_and_throw()

Query the C driver's thread-local error state. If an error is present,
retrieve the code and message, release the error object, and throw a
[`TypeDBError`](@ref).

Must be called after every C API call that can produce an error.
"""
function check_and_throw()
    FFI.check_error() || return   # false == no error

    err_ptr = FFI.get_last_error()
    code_cstr = FFI.error_code(err_ptr)
    msg_cstr  = FFI.error_message(err_ptr)

    code = code_cstr == C_NULL ? "" : unsafe_string(code_cstr)
    msg  = msg_cstr  == C_NULL ? "" : unsafe_string(msg_cstr)

    FFI.error_drop(err_ptr)
    throw(TypeDBError(code, msg))
end

# ─── Convenience macro ────────────────────────────────────────────────────────

"""
    @checkerr expr

Evaluate `expr`, then call [`check_and_throw()`](@ref).
Returns the result of `expr` on success.

```julia
handle = @checkerr FFI.driver_open(addr, creds, opts)
```
"""
macro checkerr(expr)
    quote
        local _result = $(esc(expr))
        check_and_throw()
        _result
    end
end
