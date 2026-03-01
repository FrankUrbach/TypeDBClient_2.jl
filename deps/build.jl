using Libdl, Pkg, Base.BinaryPlatforms

const LIB_NAME = Sys.isapple()   ? "libtypedb_driver_clib.dylib" :
                 Sys.islinux()   ? "libtypedb_driver_clib.so"    :
                 Sys.iswindows() ? "typedb_driver_clib.dll"       :
                 error("Unsupported platform: $(Sys.MACHINE)")

const PKG_ROOT       = dirname(@__DIR__)
const ARTIFACTS_TOML = joinpath(PKG_ROOT, "Artifacts.toml")

function build_from_source(src_root)
    manifest = joinpath(src_root, "c", "Cargo.toml")
    isfile(manifest) || error("Cargo.toml not found at $manifest")
    @info "Building typedb_driver_clib via cargo (this may take a few minutes)…"
    run(`cargo build --manifest-path $manifest --release`)
    lib = joinpath(src_root, "target", "release", LIB_NAME)
    isfile(lib) || error("Library not found after build: $lib")
    abspath(lib)
end

lib_path = let
    override  = get(ENV, "TYPEDB_DRIVER_LIB", "")
    src_root  = get(ENV, "TYPEDB_DRIVER_SRC", "")
    local_lib = joinpath(@__DIR__, "usr", "lib", LIB_NAME)

    if !isempty(override)
        abspath(override)
    elseif !isempty(src_root)
        build_from_source(src_root)
    elseif isfile(local_lib)
        @info "Using existing library from deps/usr/lib/"
        abspath(local_lib)
    else
        # Check if already registered and valid
        h = Pkg.Artifacts.artifact_hash("TypeDBClient_jll", ARTIFACTS_TOML)
        if h !== nothing && Pkg.Artifacts.artifact_exists(h)
            candidate = joinpath(Pkg.Artifacts.artifact_path(h), "lib", LIB_NAME)
            if isfile(candidate)
                @info "Artifact already valid – skipping rebuild."
                exit(0)
            end
        end
        error("""
        TypeDB driver library not found. Set one of:
          TYPEDB_DRIVER_SRC  – typedb-driver repo root (builds via cargo)
          TYPEDB_DRIVER_LIB  – pre-built $(LIB_NAME) path
        """)
    end
end

hash = Pkg.Artifacts.create_artifact() do dir
    mkpath(joinpath(dir, "lib"))
    cp(lib_path, joinpath(dir, "lib", LIB_NAME); force=true)
end

Pkg.Artifacts.bind_artifact!(ARTIFACTS_TOML, "TypeDBClient_jll", hash;
                              platform=HostPlatform(), force=true)
@info "Artifact registered → $(joinpath(Pkg.Artifacts.artifact_path(hash), "lib", LIB_NAME))"
