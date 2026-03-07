#!/usr/bin/env bash
set -euo pipefail

ROOT_ENV="/home/Projetos/ProjetosDocker/.env"
if [[ -f "$ROOT_ENV" ]]; then
  set -a
  source "$ROOT_ENV"
  set +a
fi

: "${DREMIO_HOST:?DREMIO_HOST não definido}"
: "${DREMIO_USER:?DREMIO_USER não definido}"
: "${DREMIO_PASSWORD:?DREMIO_PASSWORD não definido}"
: "${DREMIO_SQL_FILE:?DREMIO_SQL_FILE não definido}"

if [[ ! -f "$DREMIO_SQL_FILE" ]]; then
  echo "Arquivo SQL não encontrado: $DREMIO_SQL_FILE" >&2
  exit 1
fi

SQL_PAYLOAD=$(python3 - <<'PY'
import json, os
p = os.environ['DREMIO_SQL_FILE']
with open(p, 'r', encoding='utf-8') as f:
    sql = f.read()
print(json.dumps({"sql": sql}))
PY
)

LOGIN_RESP=$(curl -sS -X POST "$DREMIO_HOST/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASSWORD\"}")

TOKEN=$(/home/Projetos/ProjetosDocker/.venv/bin/python - <<'PY'
import json, os
resp = os.environ.get("LOGIN_RESP", "")
try:
    data = json.loads(resp)
except Exception:
    print("")
    raise SystemExit(0)
print(data.get("token", ""))
PY
)

if [[ -z "$TOKEN" ]]; then
  echo "Falha no login do Dremio. Resposta recebida:" >&2
  echo "$LOGIN_RESP" >&2
  echo "Verifique DREMIO_USER e DREMIO_PASSWORD em /home/Projetos/ProjetosDocker/.env" >&2
  exit 1
fi

RESP=$(curl -sS -X POST "$DREMIO_HOST/api/v3/sql" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio${TOKEN}" \
  -d "$SQL_PAYLOAD")

echo "$RESP"
