# Raw ccall declarations for every exported C function in libtypedb_driver_clib.
# The `libtypedb` constant is defined in deps/deps.jl and must be loaded first.
#
# Naming convention:
#   Julia name  ==  C function name  (no prefix stripping at this layer)
#
# All pointers are typed as the appropriate Handle alias from types.jl.
# Strings passed *to* C are Cstring; strings returned *from* C are Cstring.

module FFI

using ..TypeDBClient: libtypedb

# All opaque handle types are Ptr{Cvoid} at the C ABI boundary.
# Redeclare them locally so this sub-module is self-contained.
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

# ─── Logging ────────────────────────────────────────────────────────────────────

init_logging() =
    ccall((:init_logging, libtypedb), Cvoid, ())

# ─── Credentials ────────────────────────────────────────────────────────────────

credentials_new(username::Cstring, password::Cstring) =
    ccall((:credentials_new, libtypedb), CredentialsHandle, (Cstring, Cstring), username, password)

credentials_drop(ptr::CredentialsHandle) =
    ccall((:credentials_drop, libtypedb), Cvoid, (CredentialsHandle,), ptr)

# ─── DriverOptions ───────────────────────────────────────────────────────────────

driver_options_new(tls_enabled::Bool, tls_root_ca::Ptr{UInt8}) =
    ccall((:driver_options_new, libtypedb), DriverOptionsHandle,
          (Bool, Ptr{UInt8}), tls_enabled, tls_root_ca)

driver_options_drop(ptr::DriverOptionsHandle) =
    ccall((:driver_options_drop, libtypedb), Cvoid, (DriverOptionsHandle,), ptr)

# ─── Driver ─────────────────────────────────────────────────────────────────────

driver_open(address::Cstring, creds::CredentialsHandle, opts::DriverOptionsHandle) =
    ccall((:driver_open, libtypedb), TypeDBDriverHandle,
          (Cstring, CredentialsHandle, DriverOptionsHandle), address, creds, opts)

driver_open_with_description(address::Cstring, creds::CredentialsHandle,
                             opts::DriverOptionsHandle, lang::Cstring) =
    ccall((:driver_open_with_description, libtypedb), TypeDBDriverHandle,
          (Cstring, CredentialsHandle, DriverOptionsHandle, Cstring), address, creds, opts, lang)

driver_close(ptr::TypeDBDriverHandle) =
    ccall((:driver_close, libtypedb), Cvoid, (TypeDBDriverHandle,), ptr)

driver_is_open(ptr::TypeDBDriverHandle) =
    ccall((:driver_is_open, libtypedb), Bool, (TypeDBDriverHandle,), ptr)

driver_force_close(ptr::TypeDBDriverHandle) =
    ccall((:driver_force_close, libtypedb), Cvoid, (TypeDBDriverHandle,), ptr)

# ─── Error ──────────────────────────────────────────────────────────────────────

check_error() =
    ccall((:check_error, libtypedb), Bool, ())

get_last_error() =
    ccall((:get_last_error, libtypedb), ErrorHandle, ())

error_code(err::ErrorHandle) =
    ccall((:error_code, libtypedb), Cstring, (ErrorHandle,), err)

error_message(err::ErrorHandle) =
    ccall((:error_message, libtypedb), Cstring, (ErrorHandle,), err)

error_drop(err::ErrorHandle) =
    ccall((:error_drop, libtypedb), Cvoid, (ErrorHandle,), err)

# ─── String memory ───────────────────────────────────────────────────────────────

string_free(ptr::Cstring) =
    ccall((:string_free, libtypedb), Cvoid, (Cstring,), ptr)

# ─── String iterator ─────────────────────────────────────────────────────────────

string_iterator_next(it::StringIterHandle) =
    ccall((:string_iterator_next, libtypedb), Cstring, (StringIterHandle,), it)

string_iterator_drop(it::StringIterHandle) =
    ccall((:string_iterator_drop, libtypedb), Cvoid, (StringIterHandle,), it)

# ─── Database ────────────────────────────────────────────────────────────────────

database_get_name(db::DatabaseHandle) =
    ccall((:database_get_name, libtypedb), Cstring, (DatabaseHandle,), db)

database_schema(db::DatabaseHandle) =
    ccall((:database_schema, libtypedb), Cstring, (DatabaseHandle,), db)

database_type_schema(db::DatabaseHandle) =
    ccall((:database_type_schema, libtypedb), Cstring, (DatabaseHandle,), db)

database_delete(db::DatabaseHandle) =
    ccall((:database_delete, libtypedb), Cvoid, (DatabaseHandle,), db)

database_close(db::DatabaseHandle) =
    ccall((:database_close, libtypedb), Cvoid, (DatabaseHandle,), db)

database_export_to_file(db::DatabaseHandle, schema_file::Cstring, data_file::Cstring) =
    ccall((:database_export_to_file, libtypedb), Cvoid,
          (DatabaseHandle, Cstring, Cstring), db, schema_file, data_file)

# ─── Database manager (DatabaseIterator) ─────────────────────────────────────────

databases_all(driver::TypeDBDriverHandle) =
    ccall((:databases_all, libtypedb), DatabaseIterHandle, (TypeDBDriverHandle,), driver)

databases_create(driver::TypeDBDriverHandle, name::Cstring) =
    ccall((:databases_create, libtypedb), Cvoid, (TypeDBDriverHandle, Cstring), driver, name)

databases_contains(driver::TypeDBDriverHandle, name::Cstring) =
    ccall((:databases_contains, libtypedb), Bool, (TypeDBDriverHandle, Cstring), driver, name)

databases_get(driver::TypeDBDriverHandle, name::Cstring) =
    ccall((:databases_get, libtypedb), DatabaseHandle, (TypeDBDriverHandle, Cstring), driver, name)

databases_import_from_file(driver::TypeDBDriverHandle, name::Cstring,
                           schema::Cstring, data_file::Cstring) =
    ccall((:databases_import_from_file, libtypedb), Cvoid,
          (TypeDBDriverHandle, Cstring, Cstring, Cstring), driver, name, schema, data_file)

database_iterator_next(it::DatabaseIterHandle) =
    ccall((:database_iterator_next, libtypedb), DatabaseHandle, (DatabaseIterHandle,), it)

database_iterator_drop(it::DatabaseIterHandle) =
    ccall((:database_iterator_drop, libtypedb), Cvoid, (DatabaseIterHandle,), it)

# ─── TransactionOptions ──────────────────────────────────────────────────────────

transaction_options_new() =
    ccall((:transaction_options_new, libtypedb), TransactionOptionsHandle, ())

transaction_options_drop(opts::TransactionOptionsHandle) =
    ccall((:transaction_options_drop, libtypedb), Cvoid, (TransactionOptionsHandle,), opts)

transaction_options_set_transaction_timeout_millis(opts::TransactionOptionsHandle, ms::Int64) =
    ccall((:transaction_options_set_transaction_timeout_millis, libtypedb), Cvoid,
          (TransactionOptionsHandle, Int64), opts, ms)

transaction_options_get_transaction_timeout_millis(opts::TransactionOptionsHandle) =
    ccall((:transaction_options_get_transaction_timeout_millis, libtypedb), Int64,
          (TransactionOptionsHandle,), opts)

transaction_options_has_transaction_timeout_millis(opts::TransactionOptionsHandle) =
    ccall((:transaction_options_has_transaction_timeout_millis, libtypedb), Bool,
          (TransactionOptionsHandle,), opts)

transaction_options_set_schema_lock_acquire_timeout_millis(opts::TransactionOptionsHandle, ms::Int64) =
    ccall((:transaction_options_set_schema_lock_acquire_timeout_millis, libtypedb), Cvoid,
          (TransactionOptionsHandle, Int64), opts, ms)

# ─── QueryOptions ────────────────────────────────────────────────────────────────

query_options_new() =
    ccall((:query_options_new, libtypedb), QueryOptionsHandle, ())

query_options_drop(opts::QueryOptionsHandle) =
    ccall((:query_options_drop, libtypedb), Cvoid, (QueryOptionsHandle,), opts)

query_options_set_include_instance_types(opts::QueryOptionsHandle, v::Bool) =
    ccall((:query_options_set_include_instance_types, libtypedb), Cvoid,
          (QueryOptionsHandle, Bool), opts, v)

query_options_set_prefetch_size(opts::QueryOptionsHandle, n::Int64) =
    ccall((:query_options_set_prefetch_size, libtypedb), Cvoid,
          (QueryOptionsHandle, Int64), opts, n)

query_options_set_include_query_structure(opts::QueryOptionsHandle, v::Bool) =
    ccall((:query_options_set_include_query_structure, libtypedb), Cvoid,
          (QueryOptionsHandle, Bool), opts, v)

# ─── Transaction ─────────────────────────────────────────────────────────────────

transaction_new(driver::TypeDBDriverHandle, db_name::Cstring,
                tx_type::Int32, opts::TransactionOptionsHandle) =
    ccall((:transaction_new, libtypedb), TransactionHandle,
          (TypeDBDriverHandle, Cstring, Int32, TransactionOptionsHandle),
          driver, db_name, tx_type, opts)

transaction_commit(txn::TransactionHandle) =
    ccall((:transaction_commit, libtypedb), VoidPromHandle, (TransactionHandle,), txn)

transaction_rollback(txn::TransactionHandle) =
    ccall((:transaction_rollback, libtypedb), VoidPromHandle, (TransactionHandle,), txn)

transaction_close(txn::TransactionHandle) =
    ccall((:transaction_close, libtypedb), VoidPromHandle, (TransactionHandle,), txn)

transaction_drop_sync(txn::TransactionHandle) =
    ccall((:transaction_drop_sync, libtypedb), Cvoid, (TransactionHandle,), txn)

transaction_is_open(txn::TransactionHandle) =
    ccall((:transaction_is_open, libtypedb), Bool, (TransactionHandle,), txn)

transaction_query(txn::TransactionHandle, query_str::Cstring, opts::QueryOptionsHandle) =
    ccall((:transaction_query, libtypedb), QueryAnswerPromHandle,
          (TransactionHandle, Cstring, QueryOptionsHandle), txn, query_str, opts)

# ─── VoidPromise ────────────────────────────────────────────────────────────────

void_promise_resolve(prom::VoidPromHandle) =
    ccall((:void_promise_resolve, libtypedb), Cvoid, (VoidPromHandle,), prom)

void_promise_drop(prom::VoidPromHandle) =
    ccall((:void_promise_drop, libtypedb), Cvoid, (VoidPromHandle,), prom)

# ─── QueryAnswer promise + answer ────────────────────────────────────────────────

query_answer_promise_resolve(prom::QueryAnswerPromHandle) =
    ccall((:query_answer_promise_resolve, libtypedb), QueryAnswerHandle,
          (QueryAnswerPromHandle,), prom)

query_answer_promise_drop(prom::QueryAnswerPromHandle) =
    ccall((:query_answer_promise_drop, libtypedb), Cvoid, (QueryAnswerPromHandle,), prom)

query_answer_get_query_type(ans::QueryAnswerHandle) =
    ccall((:query_answer_get_query_type, libtypedb), Int32, (QueryAnswerHandle,), ans)

query_answer_is_ok(ans::QueryAnswerHandle) =
    ccall((:query_answer_is_ok, libtypedb), Bool, (QueryAnswerHandle,), ans)

query_answer_is_concept_row_stream(ans::QueryAnswerHandle) =
    ccall((:query_answer_is_concept_row_stream, libtypedb), Bool, (QueryAnswerHandle,), ans)

query_answer_is_concept_document_stream(ans::QueryAnswerHandle) =
    ccall((:query_answer_is_concept_document_stream, libtypedb), Bool, (QueryAnswerHandle,), ans)

query_answer_into_rows(ans::QueryAnswerHandle) =
    ccall((:query_answer_into_rows, libtypedb), ConceptRowIterHandle,
          (QueryAnswerHandle,), ans)

query_answer_into_documents(ans::QueryAnswerHandle) =
    ccall((:query_answer_into_documents, libtypedb), StringIterHandle,
          (QueryAnswerHandle,), ans)

query_answer_drop(ans::QueryAnswerHandle) =
    ccall((:query_answer_drop, libtypedb), Cvoid, (QueryAnswerHandle,), ans)

# ─── ConceptRowIterator ───────────────────────────────────────────────────────────

concept_row_iterator_next(it::ConceptRowIterHandle) =
    ccall((:concept_row_iterator_next, libtypedb), ConceptRowHandle,
          (ConceptRowIterHandle,), it)

concept_row_iterator_drop(it::ConceptRowIterHandle) =
    ccall((:concept_row_iterator_drop, libtypedb), Cvoid, (ConceptRowIterHandle,), it)

# ─── ConceptRow ───────────────────────────────────────────────────────────────────

concept_row_drop(row::ConceptRowHandle) =
    ccall((:concept_row_drop, libtypedb), Cvoid, (ConceptRowHandle,), row)

concept_row_get_column_names(row::ConceptRowHandle) =
    ccall((:concept_row_get_column_names, libtypedb), StringIterHandle,
          (ConceptRowHandle,), row)

concept_row_get_query_type(row::ConceptRowHandle) =
    ccall((:concept_row_get_query_type, libtypedb), Int32, (ConceptRowHandle,), row)

concept_row_get_concepts(row::ConceptRowHandle) =
    ccall((:concept_row_get_concepts, libtypedb), ConceptIterHandle,
          (ConceptRowHandle,), row)

concept_row_get(row::ConceptRowHandle, col_name::Cstring) =
    ccall((:concept_row_get, libtypedb), ConceptHandle,
          (ConceptRowHandle, Cstring), row, col_name)

concept_row_get_index(row::ConceptRowHandle, idx::Csize_t) =
    ccall((:concept_row_get_index, libtypedb), ConceptHandle,
          (ConceptRowHandle, Csize_t), row, idx)

concept_row_equals(lhs::ConceptRowHandle, rhs::ConceptRowHandle) =
    ccall((:concept_row_equals, libtypedb), Bool,
          (ConceptRowHandle, ConceptRowHandle), lhs, rhs)

concept_row_to_string(row::ConceptRowHandle) =
    ccall((:concept_row_to_string, libtypedb), Cstring, (ConceptRowHandle,), row)

# ─── ConceptIterator ─────────────────────────────────────────────────────────────

concept_iterator_next(it::ConceptIterHandle) =
    ccall((:concept_iterator_next, libtypedb), ConceptHandle,
          (ConceptIterHandle,), it)

concept_iterator_drop(it::ConceptIterHandle) =
    ccall((:concept_iterator_drop, libtypedb), Cvoid, (ConceptIterHandle,), it)

# ─── Concept ─────────────────────────────────────────────────────────────────────

concept_drop(c::ConceptHandle) =
    ccall((:concept_drop, libtypedb), Cvoid, (ConceptHandle,), c)

concept_to_string(c::ConceptHandle) =
    ccall((:concept_to_string, libtypedb), Cstring, (ConceptHandle,), c)

concept_get_label(c::ConceptHandle) =
    ccall((:concept_get_label, libtypedb), Cstring, (ConceptHandle,), c)

concept_try_get_label(c::ConceptHandle) =
    ccall((:concept_try_get_label, libtypedb), Cstring, (ConceptHandle,), c)

concept_try_get_iid(c::ConceptHandle) =
    ccall((:concept_try_get_iid, libtypedb), Cstring, (ConceptHandle,), c)

concept_try_get_value_type(c::ConceptHandle) =
    ccall((:concept_try_get_value_type, libtypedb), Cstring, (ConceptHandle,), c)

concept_try_get_value(c::ConceptHandle) =
    ccall((:concept_try_get_value, libtypedb), ConceptHandle, (ConceptHandle,), c)

# Concept kind predicates
concept_is_entity_type(c::ConceptHandle)    = ccall((:concept_is_entity_type,    libtypedb), Bool, (ConceptHandle,), c)
concept_is_relation_type(c::ConceptHandle)  = ccall((:concept_is_relation_type,  libtypedb), Bool, (ConceptHandle,), c)
concept_is_attribute_type(c::ConceptHandle) = ccall((:concept_is_attribute_type, libtypedb), Bool, (ConceptHandle,), c)
concept_is_role_type(c::ConceptHandle)      = ccall((:concept_is_role_type,      libtypedb), Bool, (ConceptHandle,), c)
concept_is_entity(c::ConceptHandle)         = ccall((:concept_is_entity,         libtypedb), Bool, (ConceptHandle,), c)
concept_is_relation(c::ConceptHandle)       = ccall((:concept_is_relation,       libtypedb), Bool, (ConceptHandle,), c)
concept_is_attribute(c::ConceptHandle)      = ccall((:concept_is_attribute,      libtypedb), Bool, (ConceptHandle,), c)
concept_is_value(c::ConceptHandle)          = ccall((:concept_is_value,          libtypedb), Bool, (ConceptHandle,), c)

# Value type predicates
concept_is_boolean(c::ConceptHandle)     = ccall((:concept_is_boolean,     libtypedb), Bool, (ConceptHandle,), c)
concept_is_integer(c::ConceptHandle)     = ccall((:concept_is_integer,     libtypedb), Bool, (ConceptHandle,), c)
concept_is_double(c::ConceptHandle)      = ccall((:concept_is_double,      libtypedb), Bool, (ConceptHandle,), c)
concept_is_string(c::ConceptHandle)      = ccall((:concept_is_string,      libtypedb), Bool, (ConceptHandle,), c)
concept_is_date(c::ConceptHandle)        = ccall((:concept_is_date,        libtypedb), Bool, (ConceptHandle,), c)
concept_is_datetime(c::ConceptHandle)    = ccall((:concept_is_datetime,    libtypedb), Bool, (ConceptHandle,), c)
concept_is_datetime_tz(c::ConceptHandle) = ccall((:concept_is_datetime_tz, libtypedb), Bool, (ConceptHandle,), c)
concept_is_duration(c::ConceptHandle)    = ccall((:concept_is_duration,    libtypedb), Bool, (ConceptHandle,), c)
concept_is_decimal(c::ConceptHandle)     = ccall((:concept_is_decimal,     libtypedb), Bool, (ConceptHandle,), c)
concept_is_struct(c::ConceptHandle)      = ccall((:concept_is_struct,      libtypedb), Bool, (ConceptHandle,), c)

# Value getters
concept_get_boolean(c::ConceptHandle)  = ccall((:concept_get_boolean,  libtypedb), Bool,   (ConceptHandle,), c)
concept_get_integer(c::ConceptHandle)  = ccall((:concept_get_integer,  libtypedb), Int64,  (ConceptHandle,), c)
concept_get_double(c::ConceptHandle)   = ccall((:concept_get_double,   libtypedb), Float64,(ConceptHandle,), c)
concept_get_string(c::ConceptHandle)   = ccall((:concept_get_string,   libtypedb), Cstring,(ConceptHandle,), c)
concept_get_date_as_seconds(c::ConceptHandle) =
    ccall((:concept_get_date_as_seconds, libtypedb), Int64, (ConceptHandle,), c)

# Instance type getters (for entity, relation, attribute instances)
entity_get_type(c::ConceptHandle)    = ccall((:entity_get_type,    libtypedb), ConceptHandle, (ConceptHandle,), c)
relation_get_type(c::ConceptHandle)  = ccall((:relation_get_type,  libtypedb), ConceptHandle, (ConceptHandle,), c)
attribute_get_type(c::ConceptHandle) = ccall((:attribute_get_type, libtypedb), ConceptHandle, (ConceptHandle,), c)

end # module FFI
