#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null && pwd )"
cd "${REPO_ROOT}"

VENV_ACTIVATE="${VENV_ACTIVATE:-/home/admin/mh/fluss-r/.venv/bin/activate}"
if [[ -f "${VENV_ACTIVATE}" ]]; then
    # shellcheck disable=SC1090
    source "${VENV_ACTIVATE}"
fi

RUN_ROOT="${RUN_ROOT:-${REPO_ROOT}/.ignore/soak-runs}"
mkdir -p "${RUN_ROOT}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${RUN_ROOT}/${RUN_ID}"
mkdir -p "${RUN_DIR}"

BROKERS="${SOAK_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${SOAK_TOPIC:-soak-basic-rw-readiness}"
RATE="${SOAK_RATE:-20}"
DURATION_SECONDS="${SOAK_DURATION_SECONDS:-259200}"
MAX_NO_PROGRESS_SECONDS="${SOAK_MAX_NO_PROGRESS_SECONDS:-1800}"
CHECK_INTERVAL_SECONDS="${SOAK_CHECK_INTERVAL_SECONDS:-10}"
DIAGNOSTIC_INTERVAL_SECONDS="${SOAK_DIAGNOSTIC_INTERVAL_SECONDS:-300}"
CREATE_TOPIC_TIMEOUT_SECONDS="${SOAK_CREATE_TOPIC_TIMEOUT_SECONDS:-30}"
SKIP_TOPIC_CREATE="${SOAK_SKIP_TOPIC_CREATE:-0}"
CLIENT_MODE="${SOAK_CLIENT_MODE:-r}"
WAIT_BROKER="${SOAK_WAIT_BROKER:-1}"
BROKER_WAIT_INTERVAL_SECONDS="${SOAK_BROKER_WAIT_INTERVAL_SECONDS:-30}"

LOG_FILE="${RUN_DIR}/soak.log"
DIAG_FILE="${RUN_DIR}/diagnostics.jsonl"
PID_FILE="${RUN_DIR}/pid"
CMD_FILE="${RUN_DIR}/cmd"
LAUNCHER_FILE="${RUN_DIR}/launcher.sh"

LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="/home/admin/mh/kafka/_cmake_install/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"

cmd=(
    python tests/soak/soakclient.py
    -b "${BROKERS}"
    -t "${TOPIC}"
    -r "${RATE}"
    --duration-seconds "${DURATION_SECONDS}"
    --max-no-progress-seconds "${MAX_NO_PROGRESS_SECONDS}"
    --health-check-interval-seconds "${CHECK_INTERVAL_SECONDS}"
    --diagnostic-interval-seconds "${DIAGNOSTIC_INTERVAL_SECONDS}"
    --diagnostic-file "${DIAG_FILE}"
    --create-topic-timeout-seconds "${CREATE_TOPIC_TIMEOUT_SECONDS}"
    --client-mode "${CLIENT_MODE}"
)

if [[ "${SKIP_TOPIC_CREATE}" == "1" ]]; then
    cmd+=(--skip-topic-create)
fi

printf '%q ' "${cmd[@]}" > "${CMD_FILE}"
echo >> "${CMD_FILE}"

cat > "${LAUNCHER_FILE}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
BROKERS="__BROKERS__"
WAIT_BROKER="__WAIT_BROKER__"
BROKER_WAIT_INTERVAL_SECONDS="__BROKER_WAIT_INTERVAL_SECONDS__"
CMD_FILE="__CMD_FILE__"
LOG_FILE="__LOG_FILE__"

if [[ "${WAIT_BROKER}" == "1" ]]; then
  host="${BROKERS%%:*}"
  port="${BROKERS##*:}"
  while true; do
    if timeout 2 bash -lc "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      echo "$(date -Is) broker ${BROKERS} is reachable, starting soak client" >> "${LOG_FILE}"
      break
    fi
    echo "$(date -Is) waiting for broker ${BROKERS}..." >> "${LOG_FILE}"
    sleep "${BROKER_WAIT_INTERVAL_SECONDS}"
  done
fi

cmd="$(cat "${CMD_FILE}")"
eval "${cmd}" >> "${LOG_FILE}" 2>&1
EOF

sed -i "s|__BROKERS__|${BROKERS}|g" "${LAUNCHER_FILE}"
sed -i "s|__WAIT_BROKER__|${WAIT_BROKER}|g" "${LAUNCHER_FILE}"
sed -i "s|__BROKER_WAIT_INTERVAL_SECONDS__|${BROKER_WAIT_INTERVAL_SECONDS}|g" "${LAUNCHER_FILE}"
sed -i "s|__CMD_FILE__|${CMD_FILE}|g" "${LAUNCHER_FILE}"
sed -i "s|__LOG_FILE__|${LOG_FILE}|g" "${LAUNCHER_FILE}"
chmod +x "${LAUNCHER_FILE}"

setsid "${LAUNCHER_FILE}" > "${RUN_DIR}/launcher.log" 2>&1 < /dev/null &
PID=$!
echo "${PID}" > "${PID_FILE}"

sleep 2
if ps -p "${PID}" >/dev/null 2>&1; then
    echo "started"
    echo "run_id=${RUN_ID}"
    echo "pid=${PID}"
    echo "log=${LOG_FILE}"
    echo "diagnostics=${DIAG_FILE}"
else
    echo "failed_to_start"
    echo "run_id=${RUN_ID}"
    echo "log=${LOG_FILE}"
    echo "diagnostics=${DIAG_FILE}"
    tail -n 40 "${LOG_FILE}" || true
    exit 1
fi
