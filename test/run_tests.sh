#!/usr/bin/env bash
# Generate the fixture, then run the Rust tests against it.
#
# The fixture is generated rather than checked in: Iceberg metadata records
# absolute paths, so a checked-in table only resolves on the machine that
# made it. `pixi run test` builds the Mojo bridge first and calls this.
set -euo pipefail
cd "$(dirname "$0")/.."

MLAKE_FIXTURE="$(python test/make_fixture.py test/fixtures)"
export MLAKE_FIXTURE
echo "fixture: $MLAKE_FIXTURE"

cargo test --manifest-path rust/Cargo.toml
