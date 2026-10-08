#!/usr/bin/env bash
# Wrapper that schedulers (cron, Autosys, Control-M ...) call.
# Usage:  bash bin/run_table.sh <env> <table_id | --all> [--dry-run] [--load-date YYYY-MM-DD]
# The exit code of this script is the exit code of the load (0 = OK).
set -euo pipefail

cd "$(dirname "$0")/.."                       # always run from the project root

if [ -f "$HOME/.dif_env" ]; then
  # shellcheck disable=SC1091
  source "$HOME/.dif_env"                     # ORACLE_USER / ORACLE_PASSWORD
else
  echo "ERROR: $HOME/.dif_env not found (see README, step 'Configure the edge node')" >&2
  exit 2
fi

if [ $# -lt 2 ]; then
  echo "Usage: bash bin/run_table.sh <env> <table_id | --all> [extra options]" >&2
  exit 2
fi

ENV_NAME="$1"; shift
TARGET="$1";   shift

if [ "$TARGET" = "--all" ]; then
  python3 -m dif.run --env "$ENV_NAME" --all "$@"
else
  python3 -m dif.run --env "$ENV_NAME" --table "$TARGET" "$@"
fi
