using Gherkin
using TypeDBClient3
using Test

# ─── typedb starts ─────────────────────────────────────────────────────────────
# TypeDB CE 3.8.1 has a background checkpoint thread that may still be running
# after a transaction commit. Deleting a database while that checkpoint is active
# causes TypeDB to panic (server bug). The sleep below gives the checkpoint time
# to complete before we delete any databases.
@step "typedb starts" function (ctx)
    Base.Libc.systemsleep(0.5)  # wait for TypeDB background checkpoint to finish
    GC.gc()                     # run Julia finalizers before connecting
    tmp = TypeDBDriver(TEST_ADDRESS)
    for db in list_databases(tmp)
        for attempt in 1:5
            try
                delete_database(db)
                break
            catch
                attempt < 5 && Base.Libc.systemsleep(0.2)
            end
        end
    end
    close(tmp)
    Base.Libc.systemsleep(0.2)  # allow TypeDB to process the deletion fully
end

# ─── connection is open ────────────────────────────────────────────────────────
@step r"^connection is open: (true|false)$" function (ctx, expected)
    if _is_true(expected)
        @test CTX.driver !== nothing && isopen(CTX.driver)
    else
        @test CTX.driver === nothing || !isopen(CTX.driver)
    end
end

# ─── connection opens with default authentication ──────────────────────────────
@step "connection opens with default authentication" function (ctx)
    CTX.driver = TypeDBDriver(TEST_ADDRESS)
end

# ─── connection closes ─────────────────────────────────────────────────────────
@step "connection closes" function (ctx)
    if CTX.driver !== nothing
        close(CTX.driver)
        # keep the reference so isopen checks work
    end
end

# ─── connection opens with wrong port / host ───────────────────────────────────
@step "connection opens with a wrong port; fails" function (ctx)
    expect_throws() do
        TypeDBDriver("localhost:9999")
    end
end

@step r"^connection opens with a wrong host; fails with a message containing: \"(.+)\"$" function (ctx, msg)
    expect_throws_msg(msg) do
        TypeDBDriver("does-not-exist-host:1729")
    end
end

# ─── connection has N databases ───────────────────────────────────────────────
@step r"^connection has (\d+) databases?$" function (ctx, n_str)
    @test length(list_databases(CTX.driver)) == parse(Int, n_str)
end

# ─── connection has / does not have database ──────────────────────────────────
@step r"^connection has database: (\S+)$" function (ctx, name)
    @test contains_database(CTX.driver, name)
end

@step r"^connection does not have database: (\S+)$" function (ctx, name)
    @test !contains_database(CTX.driver, name)
end

# datatable versions
@step "connection has databases:" function (ctx, datatable)
    for row in datatable
        @test contains_database(CTX.driver, strip(row[1]))
    end
end

@step "connection does not have databases:" function (ctx, datatable)
    for row in datatable
        @test !contains_database(CTX.driver, strip(row[1]))
    end
end

# ─── connection create database ───────────────────────────────────────────────
@step r"^connection create database: ([^;]+)$" function (ctx, name)
    create_database(CTX.driver, strip(name))
end

@step r"^connection create database: (.+); fails$" function (ctx, name)
    expect_throws() do
        create_database(CTX.driver, strip(name))
    end
end

@step r"^connection create database: (.+); fails with a message containing: \"(.+)\"$" function (ctx, name, msg)
    expect_throws_msg(msg) do
        create_database(CTX.driver, strip(name))
    end
end

@step "connection create databases:" function (ctx, datatable)
    for row in datatable
        create_database(CTX.driver, strip(row[1]))
    end
end

@step "connection create databases in parallel:" function (ctx, datatable)
    tasks = Task[]
    for row in datatable
        name = strip(row[1])
        t = @async create_database(CTX.driver, name)
        push!(tasks, t)
    end
    for t in tasks
        wait(t)
    end
end

@step "connection create database with empty name; fails" function (ctx)
    expect_throws() do
        create_database(CTX.driver, "")
    end
end

@step r"^connection create database with empty name; fails with a message containing: \"(.+)\"$" function (ctx, msg)
    expect_throws_msg(msg) do
        create_database(CTX.driver, "")
    end
end

# ─── connection delete database ───────────────────────────────────────────────
@step r"^connection delete database: ([^;]+)$" function (ctx, name)
    delete_database(CTX.driver, strip(name))
end

@step r"^connection delete database: (.+); fails$" function (ctx, name)
    expect_throws() do
        delete_database(CTX.driver, strip(name))
    end
end

@step r"^connection delete database: (.+); fails with a message containing: \"(.+)\"$" function (ctx, name, msg)
    expect_throws_msg(msg) do
        delete_database(CTX.driver, strip(name))
    end
end

@step "connection delete databases:" function (ctx, datatable)
    for row in datatable
        delete_database(CTX.driver, strip(row[1]))
    end
end

@step "connection delete databases in parallel:" function (ctx, datatable)
    tasks = Task[]
    for row in datatable
        name = strip(row[1])
        t = @async delete_database(CTX.driver, name)
        push!(tasks, t)
    end
    for t in tasks
        wait(t)
    end
end

# ─── Background tasks ─────────────────────────────────────────────────────────
@step r"^in background, connection create database: (.+)$" function (ctx, name)
    t = @async create_database(CTX.driver, strip(name))
    push!(CTX.background_tasks, t)
    # Give the task a chance to start/complete
    for t2 in CTX.background_tasks
        wait(t2)
    end
    empty!(CTX.background_tasks)
end

@step r"^in background, connection delete database: (.+)$" function (ctx, name)
    t = @async delete_database(CTX.driver, strip(name))
    push!(CTX.background_tasks, t)
    for t2 in CTX.background_tasks
        wait(t2)
    end
    empty!(CTX.background_tasks)
end

@step r"^in background, connection open schema transaction for database: (.+)$" function (ctx, name)
    drv = CTX.driver
    # Determine hold strategy based on whether we want the background lock to
    # BLOCK the main thread (Part 1: schema_lock < tx_timeout) or release
    # before the main thread opens (Part 2: schema_lock > tx_timeout).
    #
    # Background: Julia's @async uses cooperative scheduling.  When a ccall
    # (blocking Rust call) is in progress, Julia's scheduler cannot switch tasks.
    # So if the background task sleeps while the main thread is blocked in
    # open_transaction's ccall, the sleep timer never fires — deadlock-like.
    # Fix: in the "should succeed" case (Part 2) we wait() for the background
    # task to fully complete (open + close) before returning, so the schema lock
    # is free when the main thread calls open_transaction.
    schema_ms = CTX.tx_options_schema_lock_ms
    tx_ms     = CTX.tx_options_timeout_ms
    # hold_long: background holds the lock longer than schema_lock_acquire_timeout
    # so that the main thread's open attempt times out (Part 1 of the scenario).
    hold_long = (schema_ms !== nothing && tx_ms !== nothing && schema_ms < tx_ms)
    hold_secs = hold_long ? Float64(coalesce(schema_ms, 1000) + 500) / 1000.0 : 0.0

    t = @async begin
        tx = open_transaction(drv, strip(name), TransactionType.SCHEMA)
        if hold_long
            sleep(hold_secs)
        end
        close(tx)
    end
    push!(CTX.background_tasks, t)
    if hold_long
        yield()      # let T start and acquire lock; it then sleeps so we return
    else
        wait(t)      # wait for T to finish (open+close) so lock is free for main
    end
end

# ─── Open transaction (single) ────────────────────────────────────────────────
@step r"^connection open (read|write|schema) transaction for database: ([^;]+)$" function (ctx, type_name, db_name)
    CTX.transaction = _open_tx(strip(db_name), _tx_type_from_name(type_name))
end

@step r"^connection open (read|write|schema) transaction for database: (.+); fails$" function (ctx, type_name, db_name)
    expect_throws() do
        CTX.transaction = _open_tx(strip(db_name), _tx_type_from_name(type_name))
    end
    CTX.transaction = nothing
end

@step r"^connection open (read|write|schema) transaction for database: (.+); fails with a message containing: \"(.+)\"$" function (ctx, type_name, db_name, msg)
    expect_throws_msg(msg) do
        CTX.transaction = _open_tx(strip(db_name), _tx_type_from_name(type_name))
    end
    CTX.transaction = nothing
end

# Alias: "connection open schema transaction for database: X" — just "open schema tx"
# (already matched above by the generic pattern)

# ─── Open many transactions ────────────────────────────────────────────────────
@step r"^connection open transactions for database: (.+), of type:$" function (ctx, db_name, datatable)
    empty!(CTX.transactions)
    db = strip(db_name)
    for row in datatable
        tx = _open_tx(db, _tx_type_from_name(strip(row[1])))
        push!(CTX.transactions, tx)
    end
end

@step r"^connection open transactions in parallel for database: (.+), of type:$" function (ctx, db_name, datatable)
    empty!(CTX.transactions_parallel)
    db = strip(db_name)
    drv = CTX.driver
    timeout_ms = CTX.tx_options_timeout_ms
    schema_ms  = CTX.tx_options_schema_lock_ms
    tasks = Task[]
    for row in datatable
        type_name = strip(row[1])
        t = @async open_transaction(drv, db, _tx_type_from_name(type_name);
                                    timeout_ms=timeout_ms, schema_lock_ms=schema_ms)
        push!(tasks, t)
    end
    for t in tasks
        push!(CTX.transactions_parallel, fetch(t))
    end
end

# ─── Transaction status / type checks ─────────────────────────────────────────
@step r"^transaction is open: (true|false)$" function (ctx, expected)
    if _is_true(expected)
        @test CTX.transaction !== nothing && isopen(CTX.transaction)
    else
        @test CTX.transaction === nothing || !isopen(CTX.transaction)
    end
end

@step r"^transaction has type: (read|write|schema)$" function (ctx, expected)
    @test CTX.transaction !== nothing
    @test transaction_type_name(CTX.transaction) == expected
end

@step "transactions are open: true" function (ctx)
    @test all(isopen, CTX.transactions)
end

@step "transactions have type:" function (ctx, datatable)
    @test length(CTX.transactions) == length(datatable)
    for (tx, row) in zip(CTX.transactions, datatable)
        @test transaction_type_name(tx) == strip(row[1])
    end
end

@step "transactions in parallel are open: true" function (ctx)
    @test all(isopen, CTX.transactions_parallel)
end

@step "transactions in parallel have type:" function (ctx, datatable)
    @test length(CTX.transactions_parallel) == length(datatable)
    for (tx, row) in zip(CTX.transactions_parallel, datatable)
        @test transaction_type_name(tx) == strip(row[1])
    end
end

# ─── Transaction commits / rollback / close ────────────────────────────────────
@step "transaction commits" function (ctx)
    commit(CTX.transaction)
    CTX.transaction = nothing
end

@step "transaction commits; fails" function (ctx)
    expect_throws() do
        commit(CTX.transaction)
    end
    CTX.transaction = nothing
end

@step r"^transaction commits; fails with a message containing: \"(.+)\"$" function (ctx, msg)
    expect_throws_msg(msg) do
        commit(CTX.transaction)
    end
    CTX.transaction = nothing
end

@step "transaction closes" function (ctx)
    if CTX.transaction !== nothing
        close(CTX.transaction)
        CTX.transaction = nothing
    end
end

@step "transaction rollbacks" function (ctx)
    rollback(CTX.transaction)
end

@step "transaction rollbacks; fails" function (ctx)
    expect_throws() do
        rollback(CTX.transaction)
    end
    CTX.transaction = nothing
end

@step r"^transaction rollbacks; fails with a message containing: \"(.+)\"$" function (ctx, msg)
    expect_throws_msg(msg) do
        rollback(CTX.transaction)
    end
    CTX.transaction = nothing
end

# ─── Transaction options ───────────────────────────────────────────────────────
@step r"^set transaction option transaction_timeout_millis to: (\d+)$" function (ctx, v)
    CTX.tx_options_timeout_ms = parse(Int64, v)
end

@step r"^set transaction option schema_lock_acquire_timeout_millis to: (\d+)$" function (ctx, v)
    CTX.tx_options_schema_lock_ms = parse(Int64, v)
end

# ─── Schema retrieval ─────────────────────────────────────────────────────────
@step r"^connection get database\((\w[\w-]*)\) has schema:$" function (ctx, db_name, docstring)
    db   = get_database(CTX.driver, db_name)
    schema = strip(database_schema(db))
    expected = strip(docstring.content)
    @test schema_defs_match(schema, expected)
end

@step r"^connection get database\((\w[\w-]*)\) has type schema:$" function (ctx, db_name, docstring)
    db   = get_database(CTX.driver, db_name)
    schema = strip(database_type_schema(db))
    expected = strip(docstring.content)
    @test schema_defs_match(schema, expected)
end

# ─── Timing ───────────────────────────────────────────────────────────────────
@step r"^wait (\d+) seconds?$" function (ctx, n)
    sleep(parse(Int, n))
end
