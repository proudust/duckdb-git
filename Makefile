.PHONY: clean clean_all debug_gix release_gix test_gix test_default

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
test_debug: test_extension_debug
test_release: test_extension_release

test_default: configure debug test_extension_debug

test_gix: export GIX_BACKEND := 1
test_gix: debug_gix test_extension_debug

BENCH_RESULTS_DIR ?= target/bench-results
BENCH_RESULTS_FILE ?= $(BENCH_RESULTS_DIR)/latest.md
BENCH_RESULTS_RAW_FILE ?= $(BENCH_RESULTS_DIR)/latest-raw.txt

bench:
	@mkdir -p $(BENCH_RESULTS_DIR)
	cargo bench -q --no-default-features --features bundled,libgit-backend,gix-backend 2>&1 \
		| tee $(BENCH_RESULTS_RAW_FILE) | python3 scripts/format_divan_table.py | tee $(BENCH_RESULTS_FILE)

clean: clean_build clean_rust
clean_all: clean_configure clean
