# ─── TypeDBDriver ─────────────────────────────────────────────────────────────

"""
    TypeDBDriver

Represents an open connection to a TypeDB server.  Wraps a native driver handle
and registers a finalizer to ensure the connection is closed even if the user
forgets to call `close`.

Prefer the do-block form:

```julia
TypeDBDriver("localhost:1729"; username="admin", password="password") do driver
    # use driver …
end
```
"""
mutable struct TypeDBDriver
    handle::TypeDBDriverHandle
    _closed::Bool

    function TypeDBDriver(handle::TypeDBDriverHandle)
        handle == C_NULL && error("driver_open returned NULL (check server address and credentials)")
        obj = new(handle, false)
        finalizer(obj) do d
            if !d._closed
                d._closed = true
                FFI.driver_close(d.handle)
            end
        end
        obj
    end
end

"""
    TypeDBDriver(address; username, password, tls_enabled, tls_root_ca) -> TypeDBDriver

Open a new connection to the TypeDB server at `address` (e.g. `"localhost:1729"`).

Keyword arguments:
- `username`     – defaults to `"admin"`
- `password`     – defaults to `"password"`
- `tls_enabled`  – defaults to `false`
- `tls_root_ca`  – path to CA certificate (empty string = system default); only
                   used when `tls_enabled = true`
"""
function TypeDBDriver(address::AbstractString;
                      username::AbstractString  = "admin",
                      password::AbstractString  = "password",
                      tls_enabled::Bool         = false,
                      tls_root_ca::AbstractString = "")
    creds = @checkerr FFI.credentials_new(
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, username)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, password)))

    # driver_options_new accepts NULL for tls_root_ca when TLS is disabled.
    # We use Ptr{UInt8} to allow passing C_NULL directly.
    ca_ptr = isempty(tls_root_ca) ? Ptr{UInt8}(C_NULL) :
             Base.unsafe_convert(Ptr{UInt8}, Base.cconvert(Cstring, tls_root_ca))

    opts = GC.@preserve tls_root_ca @checkerr FFI.driver_options_new(tls_enabled, ca_ptr)

    handle = GC.@preserve username password tls_root_ca begin
        @checkerr FFI.driver_open(
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, address)),
            creds, opts)
    end

    FFI.credentials_drop(creds)
    FFI.driver_options_drop(opts)

    TypeDBDriver(handle)
end

"""
    TypeDBDriver(f, address; kwargs...)

Do-block form.  Opens a connection, passes it to `f`, then closes it (even if
`f` throws).

```julia
TypeDBDriver("localhost:1729") do driver
    create_database(driver, "mydb")
end
```
"""
function TypeDBDriver(f::Function, address::AbstractString; kwargs...)
    driver = TypeDBDriver(address; kwargs...)
    try
        f(driver)
    finally
        close(driver)
    end
end

"""
    close(driver::TypeDBDriver)

Close the driver connection.  Idempotent – safe to call multiple times.
"""
function Base.close(driver::TypeDBDriver)
    if !driver._closed
        driver._closed = true
        FFI.driver_close(driver.handle)
    end
end

"""
    isopen(driver::TypeDBDriver) -> Bool
"""
Base.isopen(driver::TypeDBDriver) =
    !driver._closed && FFI.driver_is_open(driver.handle)

function Base.show(io::IO, d::TypeDBDriver)
    state = d._closed ? "closed" : "open"
    print(io, "TypeDBDriver($(state))")
end
