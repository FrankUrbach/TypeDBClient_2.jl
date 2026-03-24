# ─── Database struct ──────────────────────────────────────────────────────────

"""
    Database

Represents a TypeDB database.  Holds a reference to the parent driver and the
database handle returned by the C API.

Obtain via [`get_database`](@ref) or through iteration of [`list_databases`](@ref).
"""
mutable struct Database
    driver::TypeDBDriver        # keep driver alive as long as Database lives
    handle::DatabaseHandle
    _name::String               # cached; immutable after construction
    _closed::Bool

    function Database(driver::TypeDBDriver, handle::DatabaseHandle)
        handle == C_NULL && error("databases_get returned NULL")
        name_cstr = FFI.database_get_name(handle)
        name = typedb_string(name_cstr)
        obj = new(driver, handle, name, false)
        finalizer(obj) do db
            db._closed || (db._closed = true; FFI.database_close(db.handle))
        end
        obj
    end
end

Base.show(io::IO, db::Database) = print(io, "Database(\"$(db._name)\")")

# ─── DatabaseManager operations ───────────────────────────────────────────────

"""
    list_databases(driver) -> Vector{Database}

Return all databases known to the server.
"""
function list_databases(driver::TypeDBDriver)::Vector{Database}
    iter = @checkerr FFI.databases_all(driver.handle)
    iter == C_NULL && return Database[]
    dbs = Database[]
    try
        while true
            h = FFI.database_iterator_next(iter)
            check_and_throw()
            h == C_NULL && break
            push!(dbs, Database(driver, h))
        end
    finally
        FFI.database_iterator_drop(iter)
    end
    dbs
end

"""
    contains_database(driver, name) -> Bool

Return `true` if a database named `name` exists on the server.
"""
function contains_database(driver::TypeDBDriver, name::AbstractString)::Bool
    GC.@preserve name begin
        result = @checkerr FFI.databases_contains(
            driver.handle,
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, name)))
    end
    result
end

"""
    create_database(driver, name)

Create a new database named `name`.  Throws if it already exists.
"""
function create_database(driver::TypeDBDriver, name::AbstractString)
    GC.@preserve name begin
        @checkerr FFI.databases_create(
            driver.handle,
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, name)))
    end
    nothing
end

"""
    get_database(driver, name) -> Database

Retrieve the database named `name`.  Throws if it does not exist.
"""
function get_database(driver::TypeDBDriver, name::AbstractString)::Database
    h = GC.@preserve name begin
        @checkerr FFI.databases_get(
            driver.handle,
            Base.unsafe_convert(Cstring, Base.cconvert(Cstring, name)))
    end
    Database(driver, h)
end

"""
    delete_database(db::Database)

Permanently delete a database.
"""
function delete_database(db::Database)
    FFI.database_delete(db.handle)
    # database_delete uses take_arc() internally: the Arc reference for db.handle is
    # consumed unconditionally, whether the server-side delete succeeds or fails.
    # Mark _closed NOW so the finalizer never calls database_close on the same pointer
    # (which would decrement an already-freed Arc → use-after-free).
    db._closed = true
    check_and_throw()
    nothing
end

"""
    delete_database(driver, name)

Permanently delete the database named `name`.
"""
function delete_database(driver::TypeDBDriver, name::AbstractString)
    db = get_database(driver, name)
    delete_database(db)
end

"""
    database_name(db::Database) -> String
"""
database_name(db::Database) = db._name

"""
    database_schema(db::Database) -> String

Return the schema definition of the database as a TypeQL string.
"""
function database_schema(db::Database)::String
    cstr = @checkerr FFI.database_schema(db.handle)
    typedb_owned_string(cstr)
end

"""
    database_type_schema(db::Database) -> String

Return the type-only schema of the database.
"""
function database_type_schema(db::Database)::String
    cstr = @checkerr FFI.database_type_schema(db.handle)
    typedb_owned_string(cstr)
end
