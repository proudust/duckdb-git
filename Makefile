.PHONY: clean clean_all debug_gix release_gix test_gix

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duckdb_git

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.5.4

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version
	$(PYTHON_VENV_BIN) -m pip install pytz

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

debug_gix: configure
	$(MAKE) debug CARGO_OVERRIDE_DUCKDB_RS_FLAG="--features gix-backend"

release_gix: configure
	$(MAKE) release CARGO_OVERRIDE_DUCKDB_RS_FLAG="--features gix-backend"

test: test_debug
test_debug: configure debug test_extension_debug
test_release: configure release test_extension_release

test_gix: export GIX_BACKEND := 1
test_gix: debug_gix test_extension_debug

bench:
	cargo bench

bench_gix:
	cargo bench --no-default-features --features bundled,libgit-backend,gix-backend

clean: clean_build clean_rust
clean_all: clean_configure clean
