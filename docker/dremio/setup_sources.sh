#!/bin/sh
# =============================================================================
# Dremio Source Auto-Provisioning Script
# 1. Cria o usuário admin se ainda não existir (first-run).
# 2. Autentica e cria a fonte Nessie via API REST.
# =============================================================================

DREMIO_URL="http://dremio:9047"
# Fail fast on missing credentials instead of silently falling back to weak
# defaults (admin/password). These come from .env via the compose env_file.
DREMIO_USER="${DREMIO_ADMIN_USER:?DREMIO_ADMIN_USER must be set (see .env)}"
DREMIO_PASS="${DREMIO_ADMIN_PASSWORD:?DREMIO_ADMIN_PASSWORD must be set (see .env)}"
NESSIE_ENDPOINT="${NESSIE_URI:-http://nessie:19120/api/v2}"
# Data plane uses the scoped MinIO service account when configured (F2-1),
# falling back to root only if MINIO_SVC_* is unset.
MINIO_ACCESS_KEY="${MINIO_SVC_ACCESS_KEY:-${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set (see .env)}}"
MINIO_SECRET_KEY="${MINIO_SVC_SECRET_KEY:-${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set (see .env)}}"
MAX_RETRIES=40
RETRY_INTERVAL=10

echo ">>> [Dremio Setup] Aguardando serviços dependentes (MinIO e Nessie)..."

# ── Aguardando MinIO ──────────────────────────────────────────────────────────
for i in $(seq 1 $MAX_RETRIES); do
  MINIO_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://minio:9000/minio/health/ready")
  if [ "$MINIO_STATUS" = "200" ]; then
    echo ">>> [Dremio Setup] MinIO está pronto!"
    break
  fi
  echo ">>> [Dremio Setup] MinIO não está pronto ($i/$MAX_RETRIES). Aguardando ${RETRY_INTERVAL}s..."
  sleep $RETRY_INTERVAL
  if [ "$i" = "$MAX_RETRIES" ]; then
    echo ">>> [Dremio Setup] ERRO: MinIO não iniciou a tempo. Abortando."
    exit 1
  fi
done

# ── Aguardando Nessie ─────────────────────────────────────────────────────────
for i in $(seq 1 $MAX_RETRIES); do
  NESSIE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${NESSIE_ENDPOINT}/config")
  if [ "$NESSIE_STATUS" = "200" ]; then
    echo ">>> [Dremio Setup] Nessie está pronto!"
    break
  fi
  echo ">>> [Dremio Setup] Nessie não está pronto ($i/$MAX_RETRIES). Aguardando ${RETRY_INTERVAL}s..."
  sleep $RETRY_INTERVAL
  if [ "$i" = "$MAX_RETRIES" ]; then
    echo ">>> [Dremio Setup] ERRO: Nessie não iniciou a tempo. Abortando."
    exit 1
  fi
done

# ── Aguardando Dremio ─────────────────────────────────────────────────────────
echo ">>> [Dremio Setup] Aguardando Dremio inicializar..."

for i in $(seq 1 $MAX_RETRIES); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${DREMIO_URL}/apiv2/server_status")
  if [ "$STATUS" = "200" ]; then
    echo ">>> [Dremio Setup] Dremio está pronto!"
    break
  fi
  echo ">>> [Dremio Setup] Tentativa $i/$MAX_RETRIES (status: $STATUS). Aguardando ${RETRY_INTERVAL}s..."
  sleep $RETRY_INTERVAL
  if [ "$i" = "$MAX_RETRIES" ]; then
    echo ">>> [Dremio Setup] ERRO: Dremio não iniciou a tempo. Abortando."
    exit 1
  fi
done

# ── Passo 1: Tentar criar o primeiro usuário (first-run setup) ──────────────
echo ">>> [Dremio Setup] Tentando criar usuário admin (first-run)..."

for j in $(seq 1 5); do
  BOOTSTRAP_OUT=$(curl -s -w "\n%{http_code}" \
    -X PUT "${DREMIO_URL}/apiv2/bootstrap/firstuser" \
    -H "Content-Type: application/json" \
    -d "{
      \"userName\": \"${DREMIO_USER}\",
      \"firstName\": \"Admin\",
      \"lastName\": \"User\",
      \"email\": \"admin@lakehouse.local\",
      \"password\": \"${DREMIO_PASS}\"
    }")

  BOOTSTRAP_BODY=$(echo "$BOOTSTRAP_OUT" | head -n -1)
  BOOTSTRAP_RESPONSE=$(echo "$BOOTSTRAP_OUT" | tail -n 1)

  if [ "$BOOTSTRAP_RESPONSE" = "200" ]; then
    echo ">>> [Dremio Setup] Usuário admin criado com sucesso!"
    break
  elif [ "$BOOTSTRAP_RESPONSE" = "409" ]; then
    echo ">>> [Dremio Setup] Usuário admin já existe. Continuando..."
    break
  elif [ "$BOOTSTRAP_RESPONSE" = "400" ] && echo "$BOOTSTRAP_BODY" | grep -q "already exists"; then
    echo ">>> [Dremio Setup] Dremio já inicializado. Continuando..."
    break
  else
    echo ">>> [Dremio Setup] Tentativa $j/5: Bootstrap retornou status $BOOTSTRAP_RESPONSE"
    # Print only the error message field if present (sed strips everything else
    # to avoid leaking the request body which contains the admin password).
    BOOTSTRAP_ERR=$(echo "$BOOTSTRAP_BODY" | sed -n 's/.*"errorMessage" *: *"\([^"]*\)".*/\1/p')
    if [ -n "$BOOTSTRAP_ERR" ]; then
      echo ">>> [Dremio Setup] Erro: $BOOTSTRAP_ERR"
    fi
    if [ "$j" = "5" ]; then
      echo ">>> [Dremio Setup] Continuando para tentativa de login mesmo com erro no bootstrap..."
    else
      sleep 5
    fi
  fi
done

sleep 3

# ── Passo 2: Autenticar ────────────────────────────────────────────────────
echo ">>> [Dremio Setup] Autenticando..."
LOGIN_BODY="{\"userName\": \"${DREMIO_USER}\", \"password\": \"${DREMIO_PASS}\"}"
LOGIN_RESPONSE=$(curl -s -X POST "${DREMIO_URL}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "$LOGIN_BODY")

TOKEN=$(echo "$LOGIN_RESPONSE" | sed 's/.*"token":"\([^"]*\)".*/\1/')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "$LOGIN_RESPONSE" ]; then
  echo ">>> [Dremio Setup] ERRO: Falha na autenticação."
  # Print only the error message field if present (avoids logging the full
  # response which may include tokens or echo back the password).
  LOGIN_ERR=$(echo "$LOGIN_RESPONSE" | sed -n 's/.*"errorMessage" *: *"\([^"]*\)".*/\1/p')
  if [ -n "$LOGIN_ERR" ]; then
    echo ">>> [Dremio Setup] Erro: $LOGIN_ERR"
  fi
  exit 1
fi

echo ">>> [Dremio Setup] Autenticado! Token obtido."

# ── Passo 3: Verificar se a fonte já existe ────────────────────────────────
SOURCE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: _dremio${TOKEN}" \
  "${DREMIO_URL}/apiv2/source/lakehouse/")

if [ "$SOURCE_STATUS" = "200" ]; then
  echo ">>> [Dremio Setup] ✅ Fonte 'lakehouse' já existe. Nada a fazer."
  exit 0
fi

# ── Passo 4: Criar a fonte Nessie ──────────────────────────────────────────
echo ">>> [Dremio Setup] Criando fonte Nessie 'lakehouse'..."
CREATE_RESPONSE=$(curl -s -X PUT "${DREMIO_URL}/apiv2/source/lakehouse/" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"lakehouse\",
    \"type\": \"NESSIE\",
    \"config\": {
      \"nessieEndpoint\": \"${NESSIE_ENDPOINT}\",
      \"nessieAuthType\": \"NONE\",
      \"credentialType\": \"ACCESS_KEY\",
      \"awsAccessKey\": \"${MINIO_ACCESS_KEY}\",
      \"awsAccessSecret\": \"${MINIO_SECRET_KEY}\",
      \"awsRootPath\": \"warehouse\",
      \"secure\": false,
      \"propertyList\": [
        {\"name\": \"dremio.s3.compat\", \"value\": \"true\"},
        {\"name\": \"fs.s3a.path.style.access\", \"value\": \"true\"},
        {\"name\": \"fs.s3a.endpoint\", \"value\": \"minio:9000\"},
        {\"name\": \"fs.s3a.connection.ssl.enabled\", \"value\": \"false\"},
        {\"name\": \"fs.s3a.aws.credentials.provider\", \"value\": \"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\"}
      ]
    }
  }")

# SECURITY: do NOT print $CREATE_RESPONSE in full — the request body contains
# the MinIO access key and secret, and Dremio's response echoes the source
# config back. We extract only the success marker (name) or the error message.
if echo "$CREATE_RESPONSE" | grep -q '"name":"lakehouse"'; then
  echo ">>> [Dremio Setup] ✅ Fonte 'lakehouse' criada com sucesso!"
else
  CREATE_ERR=$(echo "$CREATE_RESPONSE" | sed -n 's/.*"errorMessage" *: *"\([^"]*\)".*/\1/p')
  if [ -n "$CREATE_ERR" ]; then
    echo ">>> [Dremio Setup] ⚠️  Erro: $CREATE_ERR"
  else
    echo ">>> [Dremio Setup] ⚠️  Criação falhou (resposta omitida por conter credenciais)."
  fi
  exit 1
fi
