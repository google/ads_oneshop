#!/bin/bash

set -e

# Also checked within run_mex.sh, but added here to minimize troubleshooting.
if [[ -z "$REGION" ]]; then
    echo '$REGION is required.' 1>&2
    exit 1
fi

SCRIPT_DIR="${BASH_SOURCE%/*}"
ROOT_DIR="${SCRIPT_DIR}/../.."

"${ROOT_DIR}/acit/run_acit.sh"
"${SCRIPT_DIR}/run_mex.sh"
