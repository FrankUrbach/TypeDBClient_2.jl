using BinaryBuilder, Pkg

name    = "TypeDBClient_jll"
version = v"3.0.0"   # TypeDB driver version – adjust to match release tag

sources = [
    GitSource(
        "https://github.com/typedb/typedb-driver.git",
        "COMMIT_SHA_HERE",   # ← replace with actual release tag SHA
    ),
]

script = raw"""
cd ${WORKSPACE}/srcdir/typedb-driver
cargo build --release --manifest-path c/Cargo.toml

mkdir -p "${libdir}"
if [[ "${target}" == *"apple-darwin"* ]]; then
    install -vm 755 target/release/libtypedb_driver_clib.dylib "${libdir}/"
elif [[ "${target}" == *"mingw"* ]]; then
    install -vm 755 target/release/typedb_driver_clib.dll "${libdir}/"
else
    install -vm 755 target/release/libtypedb_driver_clib.so "${libdir}/"
fi
"""

platforms = [
    Platform("x86_64",  "linux";   libc="glibc"),
    Platform("aarch64", "linux";   libc="glibc"),
    Platform("x86_64",  "macos"),
    Platform("aarch64", "macos"),
    Platform("x86_64",  "windows"),
]

products = [
    LibraryProduct("libtypedb_driver_clib", :libtypedb),
]

dependencies = Dependency[]

build_tarballs(ARGS, name, version, sources, platforms, products, dependencies;
               compilers     = [:c, :rust],
               julia_compat  = "1.6")
