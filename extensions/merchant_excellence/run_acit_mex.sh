#!/bin/bash

set -e

SCRIPT_DIR="${BASH_SOURCE%/*}"
ROOT_DIR="${SCRIPT_DIR}/../.."

"${ROOT_DIR}/acit/run_acit.sh"
"${SCRIPT_DIR}/run_mex.sh"
