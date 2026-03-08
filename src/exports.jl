# Public API surface of TypeDBClient
# ─────────────────────────────────────────────────────────────────────────────

# ── Core concept union ────────────────────────────────────────────────────────
export Concept

# ── Abstract bases (for dispatch / isa checks) ────────────────────────────────
export TypeDBValue
export TypeDBType
export TypeDBInstance

# ── Value helper types ────────────────────────────────────────────────────────
export Decimal
export TypeDBDuration
export TimeZoneSpec
export IANATimeZone
export FixedTimeZone
export DatetimeTZ

# ── Concrete value types ──────────────────────────────────────────────────────
export BooleanValue
export IntegerValue
export DoubleValue
export DecimalValue
export StringValue
export DateValue
export DatetimeValue
export DatetimeTZValue
export DurationValue
export StructValue

# ── Schema types ──────────────────────────────────────────────────────────────
export EntityType
export RelationType
export AttributeType
export RoleType

# ── Instance types ────────────────────────────────────────────────────────────
export Entity
export Relation
export Attribute

# ── Infrastructure types ──────────────────────────────────────────────────────
export TypeDBDriver
export Database
export Transaction
export QueryAnswer
export ConceptRow
export ConceptRowIterator
export ConceptIterator
export DocumentIterator
export TypeDBError

# ── Enums / constants ─────────────────────────────────────────────────────────
export TransactionType
export QueryType

# ── Database management ───────────────────────────────────────────────────────
export list_databases
export contains_database
export create_database
export get_database
export delete_database
export database_name
export database_schema
export database_type_schema

# ── Transactions ──────────────────────────────────────────────────────────────
export transaction
export commit
export rollback

# ── Query ─────────────────────────────────────────────────────────────────────
export query

# ── Query answer accessors ────────────────────────────────────────────────────
export is_ok
export is_row_stream
export is_document_stream
export rows
export documents

# ── ConceptRow accessors ──────────────────────────────────────────────────────
export get_concept
export column_names
export concepts

# ── Concept accessors ─────────────────────────────────────────────────────────
export get_value
export get_label
export try_get_label
export try_get_iid
export try_get_value_type
export concept_to_string

# ── Kind predicates (schema / instance) ───────────────────────────────────────
export is_entity_type
export is_relation_type
export is_attribute_type
export is_role_type
export is_entity
export is_relation
export is_attribute
export is_value
export is_type
export is_instance

# ── Value-kind predicates ─────────────────────────────────────────────────────
export is_boolean
export is_integer
export is_double
export is_decimal
export is_string_value
export is_date
export is_datetime
export is_datetime_tz
export is_duration
export is_struct_value

# ── Materialization ───────────────────────────────────────────────────────────
export materialize

# ── Error handling ────────────────────────────────────────────────────────────
export check_and_throw
export @checkerr
