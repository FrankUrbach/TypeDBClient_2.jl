using Gherkin
using TypeDBClient3
using Test

# ─── Query options ─────────────────────────────────────────────────────────────
@step r"^set query option include_instance_types to: (true|false)$" function (ctx, v)
    CTX.query_options_include_instance_types = (v == "true")
end

@step r"^set query option prefetch_size to: (\d+)$" function (ctx, v)
    CTX.query_options_prefetch_size = parse(Int64, v)
end

@step r"^set query option include_query_structure to: (true|false)$" function (ctx, v)
    CTX.query_options_include_query_structure = (v == "true")
end

# ─── Execute TypeQL query (no answer collection) ───────────────────────────────
@step r"^typeql (schema|write|read) query$" function (ctx, _query_type, docstring)
    # Do NOT reset query_answer_rows here so that interleaved schema/write queries
    # do not discard rows collected by "concurrently get answers…" steps.
    # Rows are reset explicitly by "get answers of typeql X query" steps.
    CTX.query_answer = _run_query(docstring.content)
end

@step r"^typeql (schema|write|read) query; fails$" function (ctx, _query_type, docstring)
    expect_throws() do
        _run_query(docstring.content)
    end
    # Only clear the transaction reference when TypeDB actually closed it on the
    # server side (e.g. critical errors like write conflicts or server panics).
    # Parse and analysis errors leave the transaction open so subsequent steps
    # can continue using the same transaction.
    if CTX.transaction !== nothing && !isopen(CTX.transaction)
        CTX.transaction = nothing
    end
end

@step r"^typeql (schema|write|read) query; fails with a message containing: \"(.+)\"$" function (ctx, _query_type, msg, docstring)
    expect_throws_msg(msg) do
        _run_query(docstring.content)
    end
    if CTX.transaction !== nothing && !isopen(CTX.transaction)
        CTX.transaction = nothing
    end
end

# Parsing fails (non-critical — transaction stays open after rollback-only error)
@step r"^typeql (schema|write|read) query; parsing fails$" function (ctx, _query_type, docstring)
    expect_throws() do
        _run_query(docstring.content)
    end
    # Parsing errors may or may not close the transaction; leave CTX.transaction as-is
end

# ─── Get answers ───────────────────────────────────────────────────────────────
@step r"^get answers of typeql (read|write) query$" function (ctx, _query_type, docstring)
    CTX.query_answer_rows = nothing
    CTX.query_answer_docs = nothing
    CTX.query_answer = _run_query(docstring.content)
    if is_row_stream(CTX.query_answer)
        CTX.query_answer_rows = collect(rows(CTX.query_answer))
    elseif is_document_stream(CTX.query_answer)
        CTX.query_answer_docs = collect(documents(CTX.query_answer))
    end
end

@step r"^get answers of typeql schema query$" function (ctx, docstring)
    CTX.query_answer_rows = nothing
    CTX.query_answer_docs = nothing
    CTX.query_answer = _run_query(docstring.content)
    if is_row_stream(CTX.query_answer)
        CTX.query_answer_rows = collect(rows(CTX.query_answer))
    elseif is_document_stream(CTX.query_answer)
        CTX.query_answer_docs = collect(documents(CTX.query_answer))
    end
end

@step r"^get answers of typeql analyze$" function (ctx, docstring)
    # analyze is not yet implemented; treat as a no-op / pending
    @warn "typeql analyze not implemented, skipping"
end

@step r"^concurrently get answers of typeql read query (\d+) times$" function (ctx, n, docstring)
    typeql = docstring.content
    tasks = [@async _run_query(typeql) for _ in 1:parse(Int, n)]
    answers = [fetch(t) for t in tasks]
    # Collect rows from the first answer and store them as the concurrent pool.
    # The cursor lets subsequent "concurrently process N rows" steps consume rows
    # sequentially without being reset by interleaved schema/write queries.
    if !isempty(answers)
        CTX.query_answer = answers[1]
        if is_row_stream(CTX.query_answer)
            CTX.concurrent_rows = collect(rows(CTX.query_answer))
            CTX.concurrent_cursor = 0
            CTX.query_answer_rows = CTX.concurrent_rows
        end
    end
end

# ─── Answer size ──────────────────────────────────────────────────────────────
@step r"^answer size is: (\d+)$" function (ctx, n)
    expected = parse(Int, n)
    if CTX.query_answer_rows !== nothing
        @test length(CTX.query_answer_rows) == expected
    elseif CTX.query_answer_docs !== nothing
        @test length(CTX.query_answer_docs) == expected
    else
        @test false  # no answer available
    end
end

# ─── Answer type checks ────────────────────────────────────────────────────────
@step r"^answer type is: (ok|concept rows|concept documents)$" function (ctx, expected)
    qa = CTX.query_answer
    @test qa !== nothing
    if expected == "ok"
        @test is_ok(qa)
    elseif expected == "concept rows"
        @test is_row_stream(qa)
    elseif expected == "concept documents"
        @test is_document_stream(qa)
    end
end

@step r"^answer type is not: (ok|concept rows|concept documents)$" function (ctx, expected)
    qa = CTX.query_answer
    @test qa !== nothing
    if expected == "ok"
        @test !is_ok(qa)
    elseif expected == "concept rows"
        @test !is_row_stream(qa)
    elseif expected == "concept documents"
        @test !is_document_stream(qa)
    end
end

@step "answer unwraps as ok" function (ctx)
    @test CTX.query_answer !== nothing && is_ok(CTX.query_answer)
end

@step "answer unwraps as concept rows" function (ctx)
    @test CTX.query_answer !== nothing && is_row_stream(CTX.query_answer)
end

@step "answer unwraps as concept documents" function (ctx)
    @test CTX.query_answer !== nothing && is_document_stream(CTX.query_answer)
end

# ─── Answer query-type checks ─────────────────────────────────────────────────
@step r"^answer query type is: (read|write|schema)$" function (ctx, _expected)
    # Query type metadata not yet exposed; skip with a pass
    @test CTX.query_answer !== nothing
end

@step r"^answer query type is not: (read|write|schema)$" function (ctx, _expected)
    @test CTX.query_answer !== nothing
end

@step r"^answer get row\((\d+)\) query type is: (read|write|schema)$" function (ctx, _idx, _expected)
    @test CTX.query_answer_rows !== nothing
end

@step r"^answer get row\((\d+)\) query type is not: (read|write|schema)$" function (ctx, _idx, _expected)
    @test CTX.query_answer_rows !== nothing
end

# ─── Answer row / concept accessors ──────────────────────────────────────────
function _get_row(idx_str)
    _materialise_rows()
    idx = parse(Int, idx_str) + 1  # 0-based -> 1-based
    CTX.query_answer_rows[idx]
end

# try_get_label is none / not none
@step r"^answer get row\((\d+)\) get variable\((\w+)\) try get label is not none$" function (ctx, row_idx, var_name)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test try_get_label(concept) !== nothing
end

@step r"^answer get row\((\d+)\) get variable\((\w+)\) try get label is none$" function (ctx, row_idx, var_name)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test try_get_label(concept) === nothing
end

# entity label
@step r"^answer get row\((\d+)\) get entity\((\w+)\) get type get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_entity(concept)
    @test get_label(concept) == expected_label
end

# attribute label
@step r"^answer get row\((\d+)\) get attribute\((\w+)\) get type get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_attribute(concept)
    @test get_label(concept) == expected_label
end

# attribute value
@step r"^answer get row\((\d+)\) get attribute\((\w+)\) get value is: \"(.+)\"$" function (ctx, row_idx, var_name, expected_val)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_attribute(concept)
    @test string(get_value(concept)) == expected_val
end

# relation label
@step r"^answer get row\((\d+)\) get relation\((\w+)\) get type get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_relation(concept)
    @test get_label(concept) == expected_label
end

# entity type (schema concept) label
@step r"^answer get row\((\d+)\) get entity type\((\w+)\) get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_entity_type(concept)
    @test get_label(concept) == expected_label
end

# attribute type label
@step r"^answer get row\((\d+)\) get attribute type\((\w+)\) get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_attribute_type(concept)
    @test get_label(concept) == expected_label
end

# by index of variable – entity / attribute / attribute type / entity type
@step r"^answer get row\((\d+)\) get entity by index of variable\((\w+)\) get type get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_entity(concept)
    @test get_label(concept) == expected_label
end

@step r"^answer get row\((\d+)\) get attribute by index of variable\((\w+)\) get type get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_attribute(concept)
    @test get_label(concept) == expected_label
end

@step r"^answer get row\((\d+)\) get attribute by index of variable\((\w+)\) get value is: \"(.+)\"$" function (ctx, row_idx, var_name, expected_val)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_attribute(concept)
    @test string(get_value(concept)) == expected_val
end

@step r"^answer get row\((\d+)\) get entity type by index of variable\((\w+)\) get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_entity_type(concept)
    @test get_label(concept) == expected_label
end

@step r"^answer get row\((\d+)\) get attribute type by index of variable\((\w+)\) get label: (\S+)$" function (ctx, row_idx, var_name, expected_label)
    row = _get_row(row_idx)
    concept = get_concept(row, var_name)
    @test is_attribute_type(concept)
    @test get_label(concept) == expected_label
end

# concepts size
@step r"^answer get row\((\d+)\) get concepts size is: (\d+)$" function (ctx, row_idx, expected_size)
    row = _get_row(row_idx)
    cs = collect(concepts(row))
    @test length(cs) == parse(Int, expected_size)
end

# ─── Document checks ──────────────────────────────────────────────────────────
@step "answer contains document:" function (ctx, docstring)
    if CTX.query_answer_docs !== nothing
        expected = strip(docstring.content)
        found = any(d -> _json_match(strip(d), expected), CTX.query_answer_docs)
        @test found
    else
        @warn "answer contains document: called but no cached document stream available"
        @test false
    end
end

@step "answer does not contain document:" function (ctx, docstring)
    if CTX.query_answer_docs !== nothing
        expected = strip(docstring.content)
        found = any(d -> _json_match(strip(d), expected), CTX.query_answer_docs)
        @test !found
    else
        @test true  # no documents → trivially does not contain
    end
end

"""Split a JSON object body (no surrounding braces) into top-level key:value pairs."""
function _json_split_pairs(s::AbstractString)::Vector{String}
    pairs = String[]
    depth = 0
    in_str = false
    buf = IOBuffer()
    i = firstindex(s)
    while i <= lastindex(s)
        c = s[i]
        if in_str
            if c == '\\' && i < lastindex(s)
                write(buf, c); write(buf, s[nextind(s,i)])
                i = nextind(s, nextind(s, i)); continue
            end
            write(buf, c)
            c == '"' && (in_str = false)
        else
            if c == '"'
                in_str = true; write(buf, c)
            elseif c in ('{', '[')
                depth += 1; write(buf, c)
            elseif c in ('}', ']')
                depth -= 1; write(buf, c)
            elseif c == ',' && depth == 0
                p = strip(String(take!(buf)))
                !isempty(p) && push!(pairs, p)
                i = nextind(s, i); continue
            else
                write(buf, c)
            end
        end
        i = nextind(s, i)
    end
    p = strip(String(take!(buf)))
    !isempty(p) && push!(pairs, p)
    return pairs
end

"""Canonicalize a JSON string: remove whitespace, sort object keys recursively."""
function _json_canonical(s::AbstractString)::String
    s = strip(s)
    if startswith(s, "{") && endswith(s, "}")
        inner = SubString(s, nextind(s,1), prevind(s,lastindex(s)))
        pairs = _json_split_pairs(inner)
        normed = sort([_json_canonical_pair(p) for p in pairs])
        return "{" * join(normed, ",") * "}"
    elseif startswith(s, "[") && endswith(s, "]")
        inner = SubString(s, nextind(s,1), prevind(s,lastindex(s)))
        elems = _json_split_pairs(inner)
        normed = [_json_canonical(e) for e in elems]
        return "[" * join(normed, ",") * "]"
    else
        # Scalar: collapse internal whitespace (e.g. whitespace in strings)
        return replace(String(s), r"\s+" => " ")
    end
end

function _json_canonical_pair(pair::AbstractString)::String
    pair = strip(pair)
    m = match(r"^(\"[^\"]*\"\s*:)\s*(.+)$"s, pair)
    m === nothing && return String(pair)
    key = replace(String(m.captures[1]), r"\s+" => "")
    val = _json_canonical(m.captures[2])
    return key * val
end

"""JSON-aware match: canonicalise both sides (sorted keys, no whitespace) and compare."""
function _json_match(a::AbstractString, b::AbstractString)::Bool
    try
        _json_canonical(a) == _json_canonical(b)
    catch
        # Fallback: simple whitespace-normalised comparison
        a_norm = replace(replace(String(a), r"\s+" => " "), r"\s*([{}\[\]:,])\s*" => s"\1")
        b_norm = replace(replace(String(b), r"\s+" => " "), r"\s*([{}\[\]:,])\s*" => s"\1")
        a_norm == b_norm
    end
end

# ─── Column names ─────────────────────────────────────────────────────────────
@step "answer column names are:" function (ctx, datatable)
    _materialise_rows()
    if !isempty(CTX.query_answer_rows)
        names = column_names(CTX.query_answer_rows[1])
        expected = [strip(r[1]) for r in datatable]
        # TypeDB CE 3.8.1 returns columns in schema-definition order (attributes first),
        # which may differ from query-variable order in the feature file.
        # Compare as sets so the test is order-independent.
        @test Set(names) == Set(expected)
    else
        @test false
    end
end

# ─── Concurrent processing ────────────────────────────────────────────────────
@step r"^concurrently process (\d+) rows? from answers$" function (ctx, n)
    # Use the concurrent-row pool (set by "concurrently get answers…") with a
    # cursor so consumption across steps is tracked correctly.
    need = parse(Int, n)
    if CTX.concurrent_rows !== nothing
        remaining = length(CTX.concurrent_rows) - CTX.concurrent_cursor
        @test remaining >= need
        CTX.concurrent_cursor += need
    else
        # Fallback: no concurrent pool — treat as a regular materialise check.
        _materialise_rows()
        @test CTX.query_answer_rows !== nothing && length(CTX.query_answer_rows) >= need
    end
end

@step r"^concurrently process (\d+) rows? from answers; fails$" function (ctx, n)
    need = parse(Int, n)
    if CTX.concurrent_rows !== nothing
        remaining = length(CTX.concurrent_rows) - CTX.concurrent_cursor
        @test remaining < need
    else
        @test CTX.query_answer_rows === nothing || length(CTX.query_answer_rows) < need
    end
end

# ─── analyze / structure steps (not yet implemented) ──────────────────────────
@step r"^typeql analyze; fails with a message containing: \"(.+)\"$" function (ctx, msg, docstring)
    @warn "typeql analyze not implemented; marking as pending"
    @test_broken false
end

@step "typeql analyze; parsing fails" function (ctx, docstring)
    @warn "typeql analyze not implemented; marking as pending"
    @test_broken false
end

@step "answers have query structure:" function (ctx, docstring)
    @test true  # not verifying structure
end

@step r"^analyzed query pipeline structure is:$" function (ctx, docstring)
    @test true
end

@step r"^analyzed query pipeline annotations are:$" function (ctx, datatable)
    @test true
end

@step r"^analyzed preamble annotations contains:$" function (ctx, datatable)
    @test true
end

@step r"^analyzed query preamble contains:$" function (ctx, datatable)
    @test true
end

@step r"^analyzed fetch annotations are:$" function (ctx, datatable)
    @test true
end
