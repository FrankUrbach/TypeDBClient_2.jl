# Public API surface of TypeDBClient
# ─────────────────────────────────────────────────────────────────────────────

# ── Types ────────────────────────────────────────────────────────────────────
export TypeDBDriver
export Database
export Transaction
export QueryAnswer
export ConceptRow
export ConceptRowIterator
export DocumentIterator
export Concept
export TypeDBError

# ── Enums / constants ────────────────────────────────────────────────────────
export TransactionType
export QueryType

# ── Driver ───────────────────────────────────────────────────────────────────
# (TypeDBDriver constructor and close/isopen are re-exported via Base)

# ── Database management ───────────────────────────────────────────────────────
export list_databases
export contains_database
export create_database
export get_database
export delete_database
export database_name
export database_schema
export database_type_schema

# ── Transactions ─────────────────────────────────────────────────────────────
export transaction
export commit
export rollback

# ── Query ────────────────────────────────────────────────────────────────────
export query

# ── Query answer accessors ───────────────────────────────────────────────────
export is_ok
export is_row_stream
export is_document_stream
export rows
export documents

# ── ConceptRow accessors ─────────────────────────────────────────────────────
export get_concept
export column_names

# ── Concept accessors ────────────────────────────────────────────────────────
export get_value
export get_label
export try_get_label
export try_get_iid
export try_get_value_type
export concept_to_string

# ── Concept kind predicates ───────────────────────────────────────────────────
export is_entity_type
export is_relation_type
export is_attribute_type
export is_role_type
export is_entity
export is_relation
export is_attribute
export is_value

# ── Error handling ────────────────────────────────────────────────────────────
export check_and_throw
export @checkerr
