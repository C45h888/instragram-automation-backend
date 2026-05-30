#!/bin/bash
# Thin wrapper — delegates to the unified runner
exec "$(dirname "$0")/run-all-tests.sh" --phase-3 "$@"
