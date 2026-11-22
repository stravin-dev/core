#!/usr/bin/env bash
set -euo pipefail

# Floci AWS Wire Parity Smoke Test
# Exercises real AWS CLI calls against running Floci using static dummy credentials
# (zero real AWS credentials, zero local-only mocks).
#
# If `aws` CLI is not installed on the host, automatically delegates execution into
# the pinned amazon/aws-cli container targeting the host's Floci port.

AWS_CLI_IMAGE="amazon/aws-cli:latest@sha256:b0102d2269f87d8531d931c1a59e248101ace586886e6c077058d94c1c37b846"

if ! command -v aws >/dev/null 2>&1; then
  if [ "${STRAVIN_PARITY_IN_CONTAINER:-0}" = "1" ]; then
    echo "FAIL: aws CLI not found inside container" >&2
    exit 1
  fi
  if ! command -v docker >/dev/null 2>&1; then
    echo "FAIL: neither aws CLI nor docker is available on PATH" >&2
    exit 1
  fi
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  ENDPOINT_HOST="${AWS_ENDPOINT_URL:-http://localhost:4566}"
  ENDPOINT_CONTAINER="${ENDPOINT_HOST//localhost/host.docker.internal}"
  ENDPOINT_CONTAINER="${ENDPOINT_CONTAINER//127.0.0.1/host.docker.internal}"
  exec docker run --rm \
    --add-host=host.docker.internal:host-gateway \
    -e STRAVIN_PARITY_IN_CONTAINER=1 \
    -e AWS_ENDPOINT_URL="${ENDPOINT_CONTAINER}" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}" \
    -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
    -e STRAVIN_S3_BUCKET="${STRAVIN_S3_BUCKET:-git-objects-us-east-1}" \
    -e STRAVIN_DDB_TABLE="${STRAVIN_DDB_TABLE:-stravin-core}" \
    -e STRAVIN_KMS_ALIAS="${STRAVIN_KMS_ALIAS:-alias/stravin-local-dev}" \
    -v "${SCRIPT_DIR}/floci-parity-test.sh:/scripts/floci-parity-test.sh:ro" \
    --entrypoint /bin/bash \
    "${AWS_CLI_IMAGE}" \
    /scripts/floci-parity-test.sh
fi

export AWS_PAGER=""
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"

BUCKET="${STRAVIN_S3_BUCKET:-git-objects-us-east-1}"
TABLE="${STRAVIN_DDB_TABLE:-stravin-core}"
KMS_ALIAS="${STRAVIN_KMS_ALIAS:-alias/stravin-local-dev}"
NONCE="$(date +%s)-$$"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# Configure path-style S3 addressing via temporary AWS config file (zero host ~/.aws dependency).
export AWS_CONFIG_FILE="${TMP_DIR}/aws-config"
export AWS_SHARED_CREDENTIALS_FILE="${TMP_DIR}/aws-creds"
cat >"${AWS_CONFIG_FILE}" <<EOF
[default]
region = ${AWS_DEFAULT_REGION}
s3 =
    addressing_style = path
EOF
cat >"${AWS_SHARED_CREDENTIALS_FILE}" <<EOF
[default]
aws_access_key_id = ${AWS_ACCESS_KEY_ID}
aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}
EOF

pass() { echo "[PASS] $*"; }
fail() { echo "[FAIL] $*" >&2; exit 1; }

echo "=== Floci Wire Parity Smoke Test (${AWS_ENDPOINT_URL}) ==="

# ------------------------------------------------------------------------------
# Surface 1: S3 path-style PutObject, HeadObject, GetObject, DeleteObject, 404 NoSuchKey
# ------------------------------------------------------------------------------
echo "--- [1/5] S3 path-style Put/Head/Get/Delete + 404 NoSuchKey ---"
S3_KEY="aa/local/parity-${NONCE}/packs/test.pack"
PAYLOAD_IN="${TMP_DIR}/in.bin"
PAYLOAD_OUT="${TMP_DIR}/out.bin"
printf 'PACK-PARITY-PAYLOAD-%s\n' "${NONCE}" >"${PAYLOAD_IN}"

aws s3api put-object --bucket "${BUCKET}" --key "${S3_KEY}" --body "${PAYLOAD_IN}" >/dev/null
HEAD_LEN="$(aws s3api head-object --bucket "${BUCKET}" --key "${S3_KEY}" --query 'ContentLength' --output text)"
EXPECTED_LEN="$(wc -c <"${PAYLOAD_IN}" | tr -d ' ')"
[ "${HEAD_LEN}" = "${EXPECTED_LEN}" ] || fail "S3 HeadObject ContentLength=${HEAD_LEN}, expected ${EXPECTED_LEN}"

aws s3api get-object --bucket "${BUCKET}" --key "${S3_KEY}" "${PAYLOAD_OUT}" >/dev/null
python3 -c 'import sys; sys.exit(0 if open(sys.argv[1], "rb").read() == open(sys.argv[2], "rb").read() else 1)' \
  "${PAYLOAD_IN}" "${PAYLOAD_OUT}" || fail "S3 GetObject body mismatch"

aws s3api delete-object --bucket "${BUCKET}" --key "${S3_KEY}" >/dev/null

set +e
S3_404_ERR="$(aws s3api get-object --bucket "${BUCKET}" --key "${S3_KEY}" "${TMP_DIR}/missing.bin" 2>&1)"
S3_404_RC=$?
set -e
[ "${S3_404_RC}" -ne 0 ] || fail "S3 GetObject after DeleteObject unexpectedly succeeded"
echo "${S3_404_ERR}" | grep -qE 'NoSuchKey|404|Not Found' || fail "S3 missing key error did not indicate NoSuchKey/404: ${S3_404_ERR}"
pass "S3 path-style Put/Head/Get/Delete + 404 NoSuchKey"

# ------------------------------------------------------------------------------
# Surface 2: DynamoDB TransactWriteItems success + TransactionCanceledException CancellationReasons
# ------------------------------------------------------------------------------
echo "--- [2/5] DynamoDB TransactWriteItems success & CancellationReasons ---"
RS_PK="T#local#REPO#parity-${NONCE}#RS#0"
RH_PK="T#local#REPO#parity-${NONCE}#RH#0"
MS_PK="T#local#REPO#parity-${NONCE}#MS#0"
REF_SK="REF#refs/heads/main"
HIST_SK="REF#refs/heads/main#01PARITY${NONCE}"
PACK_SK="PACK#01PARITY${NONCE}#deadbeef"
OID_1="1111111111111111111111111111111111111111"
OID_2="2222222222222222222222222222222222222222"
OID_WRONG="9999999999999999999999999999999999999999"
TTL_EPOCH="$(( $(date +%s) + 604800 ))"

cat >"${TMP_DIR}/tx-ok.json" <<EOF
[
  {
    "Put": {
      "TableName": "${TABLE}",
      "Item": {
        "PK": {"S": "${RS_PK}"},
        "SK": {"S": "${REF_SK}"},
        "oid": {"S": "${OID_1}"},
        "pack_id": {"S": "deadbeef"},
        "epoch": {"N": "1"}
      },
      "ConditionExpression": "attribute_not_exists(oid)"
    }
  },
  {
    "Put": {
      "TableName": "${TABLE}",
      "Item": {
        "PK": {"S": "${RH_PK}"},
        "SK": {"S": "${HIST_SK}"},
        "oid": {"S": "${OID_1}"},
        "pack_id": {"S": "deadbeef"},
        "ttl": {"N": "${TTL_EPOCH}"}
      }
    }
  },
  {
    "Put": {
      "TableName": "${TABLE}",
      "Item": {
        "PK": {"S": "${MS_PK}"},
        "SK": {"S": "${PACK_SK}"},
        "status": {"S": "ACTIVE"},
        "origin_region": {"S": "${AWS_DEFAULT_REGION}"}
      }
    }
  }
]
EOF

aws dynamodb transact-write-items --transact-items "file://${TMP_DIR}/tx-ok.json" >/dev/null

GOT_OID="$(aws dynamodb get-item \
  --table-name "${TABLE}" \
  --key "{\"PK\":{\"S\":\"${RS_PK}\"},\"SK\":{\"S\":\"${REF_SK}\"}}" \
  --query 'Item.oid.S' --output text)"
[ "${GOT_OID}" = "${OID_1}" ] || fail "TransactWriteItems committed item oid='${GOT_OID}', expected '${OID_1}'"

cat >"${TMP_DIR}/tx-conflict.json" <<EOF
[
  {
    "Update": {
      "TableName": "${TABLE}",
      "Key": {
        "PK": {"S": "${RS_PK}"},
        "SK": {"S": "${REF_SK}"}
      },
      "UpdateExpression": "SET oid = :new_oid",
      "ConditionExpression": "oid = :expected_oid",
      "ExpressionAttributeValues": {
        ":new_oid": {"S": "${OID_2}"},
        ":expected_oid": {"S": "${OID_WRONG}"}
      }
    }
  },
  {
    "Put": {
      "TableName": "${TABLE}",
      "Item": {
        "PK": {"S": "${RH_PK}"},
        "SK": {"S": "REF#refs/heads/main#02CONFLICT${NONCE}"},
        "oid": {"S": "${OID_2}"}
      }
    }
  }
]
EOF

set +e
TX_ERR="$(aws dynamodb transact-write-items --transact-items "file://${TMP_DIR}/tx-conflict.json" 2>&1)"
TX_RC=$?
set -e
[ "${TX_RC}" -ne 0 ] || fail "TransactWriteItems conditional conflict unexpectedly succeeded"
echo "${TX_ERR}" | grep -q 'TransactionCanceledException' || fail "Expected TransactionCanceledException, got: ${TX_ERR}"
echo "${TX_ERR}" | grep -q 'ConditionalCheckFailed' || fail "Expected populated CancellationReasons with ConditionalCheckFailed, got: ${TX_ERR}"
pass "DynamoDB TransactWriteItems success + TransactionCanceledException with CancellationReasons"

# ------------------------------------------------------------------------------
# Surface 3: DynamoDB UpdateTimeToLive on 'ttl'
# ------------------------------------------------------------------------------
echo "--- [3/5] DynamoDB UpdateTimeToLive on ttl ---"
TTL_JSON="$(aws dynamodb describe-time-to-live --table-name "${TABLE}" --output json)"
TTL_STATE="$(echo "${TTL_JSON}" | jq -r '.TimeToLiveDescription.TimeToLiveStatus // empty')"
TTL_ATTR="$(echo "${TTL_JSON}" | jq -r '.TimeToLiveDescription.AttributeName // empty')"
[ "${TTL_STATE}" = "ENABLED" ] || fail "Expected TTL status ENABLED on ${TABLE}, got '${TTL_STATE}'"
[ "${TTL_ATTR}" = "ttl" ] || fail "Expected TTL AttributeName=ttl on ${TABLE}, got '${TTL_ATTR}'"

# Also exercise UpdateTimeToLive directly on an ephemeral parity table.
TTL_TABLE="stravin-parity-ttl-${NONCE}"
aws dynamodb create-table \
  --table-name "${TTL_TABLE}" \
  --attribute-definitions AttributeName=PK,AttributeType=S \
  --key-schema AttributeName=PK,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST >/dev/null
aws dynamodb update-time-to-live \
  --table-name "${TTL_TABLE}" \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null
TTL_VERIFY="$(aws dynamodb describe-time-to-live --table-name "${TTL_TABLE}" --query 'TimeToLiveDescription.TimeToLiveStatus' --output text)"
aws dynamodb delete-table --table-name "${TTL_TABLE}" >/dev/null
[ "${TTL_VERIFY}" = "ENABLED" ] || fail "UpdateTimeToLive on ${TTL_TABLE} yielded status '${TTL_VERIFY}'"
pass "DynamoDB UpdateTimeToLive on ttl"

# ------------------------------------------------------------------------------
# Surface 4: DynamoDB Streams NEW_IMAGE from TransactWriteItems
# ------------------------------------------------------------------------------
echo "--- [4/5] DynamoDB Streams (DescribeStream/GetShardIterator/GetRecords) from TransactWriteItems ---"
STREAM_ARN="$(aws dynamodb describe-table \
  --table-name "${TABLE}" \
  --query 'Table.LatestStreamArn' --output text)"
[ -n "${STREAM_ARN}" ] && [ "${STREAM_ARN}" != "None" ] || fail "LatestStreamArn missing on table ${TABLE}"

SHARD_IDS="$(aws dynamodbstreams describe-stream \
  --stream-arn "${STREAM_ARN}" \
  --query 'StreamDescription.Shards[].ShardId' --output text)"
[ -n "${SHARD_IDS}" ] || fail "DescribeStream returned no shards for ${STREAM_ARN}"

FOUND_STREAM_RECORD=0
for SHARD_ID in ${SHARD_IDS}; do
  ITERATOR="$(aws dynamodbstreams get-shard-iterator \
    --stream-arn "${STREAM_ARN}" \
    --shard-id "${SHARD_ID}" \
    --shard-iterator-type TRIM_HORIZON \
    --query 'ShardIterator' --output text)"
  [ -n "${ITERATOR}" ] && [ "${ITERATOR}" != "None" ] || continue

  # Poll up to 5 pages on this shard to find our TransactWriteItems Ref History NEW_IMAGE record.
  PAGE=0
  while [ "${PAGE}" -lt 5 ] && [ -n "${ITERATOR}" ] && [ "${ITERATOR}" != "null" ]; do
    RECORDS_JSON="$(aws dynamodbstreams get-records --shard-iterator "${ITERATOR}" --output json)"
    MATCH="$(echo "${RECORDS_JSON}" | jq -r \
      --arg pk "${RH_PK}" --arg sk "${HIST_SK}" --arg oid "${OID_1}" \
      '[.Records[] | select(.dynamodb.NewImage.PK.S == $pk and .dynamodb.NewImage.SK.S == $sk and .dynamodb.NewImage.oid.S == $oid)] | length')"
    if [ "${MATCH}" -ge 1 ]; then
      FOUND_STREAM_RECORD=1
      break 2
    fi
    ITERATOR="$(echo "${RECORDS_JSON}" | jq -r '.NextShardIterator // empty')"
    RECORD_COUNT="$(echo "${RECORDS_JSON}" | jq -r '.Records | length')"
    [ "${RECORD_COUNT}" -eq 0 ] && break
    PAGE=$((PAGE + 1))
  done
done

[ "${FOUND_STREAM_RECORD}" -eq 1 ] || fail "DynamoDB Streams did not emit a NEW_IMAGE record for TransactWriteItems item PK=${RH_PK} SK=${HIST_SK}"
pass "DynamoDB Streams NEW_IMAGE record verified for TransactWriteItems"

# ------------------------------------------------------------------------------
# Surface 5: KMS CreateKey, CreateAlias, GenerateDataKey, Decrypt
# ------------------------------------------------------------------------------
echo "--- [5/5] KMS CreateKey, CreateAlias, GenerateDataKey, Decrypt ---"
PARITY_ALIAS="alias/stravin-parity-${NONCE}"
KEY_ID="$(aws kms create-key \
  --description "Stravin parity smoke test key" \
  --key-usage ENCRYPT_DECRYPT \
  --key-spec SYMMETRIC_DEFAULT \
  --query 'KeyMetadata.KeyId' --output text)"
[ -n "${KEY_ID}" ] && [ "${KEY_ID}" != "None" ] || fail "KMS CreateKey returned empty KeyId"

aws kms create-alias --alias-name "${PARITY_ALIAS}" --target-key-id "${KEY_ID}"

GDK_JSON="$(aws kms generate-data-key --key-id "${PARITY_ALIAS}" --key-spec AES_256 --output json)"
PLAIN_B64="$(echo "${GDK_JSON}" | jq -r '.Plaintext // empty')"
CIPHER_B64="$(echo "${GDK_JSON}" | jq -r '.CiphertextBlob // empty')"
[ -n "${PLAIN_B64}" ] || fail "KMS GenerateDataKey returned empty Plaintext"
[ -n "${CIPHER_B64}" ] || fail "KMS GenerateDataKey returned empty CiphertextBlob"

python3 -c 'import base64, sys; open(sys.argv[2], "wb").write(base64.b64decode(sys.argv[1]))' \
  "${CIPHER_B64}" "${TMP_DIR}/cipher.bin"
DECRYPTED_B64="$(aws kms decrypt --ciphertext-blob "fileb://${TMP_DIR}/cipher.bin" --query 'Plaintext' --output text)"
[ "${DECRYPTED_B64}" = "${PLAIN_B64}" ] || fail "KMS Decrypt Plaintext mismatch against GenerateDataKey"

# Also verify bootstrapped alias/stravin-local-dev works end-to-end.
BOOT_GDK="$(aws kms generate-data-key --key-id "${KMS_ALIAS}" --key-spec AES_256 --output json)"
BOOT_PLAIN="$(echo "${BOOT_GDK}" | jq -r '.Plaintext')"
BOOT_CIPHER="$(echo "${BOOT_GDK}" | jq -r '.CiphertextBlob')"
python3 -c 'import base64, sys; open(sys.argv[2], "wb").write(base64.b64decode(sys.argv[1]))' \
  "${BOOT_CIPHER}" "${TMP_DIR}/boot-cipher.bin"
BOOT_DEC="$(aws kms decrypt --ciphertext-blob "fileb://${TMP_DIR}/boot-cipher.bin" --query 'Plaintext' --output text)"
[ "${BOOT_DEC}" = "${BOOT_PLAIN}" ] || fail "KMS Decrypt mismatch for bootstrapped ${KMS_ALIAS}"

aws kms delete-alias --alias-name "${PARITY_ALIAS}" >/dev/null 2>&1 || true
pass "KMS CreateKey, CreateAlias, GenerateDataKey, Decrypt"

# Cleanup test DDB items
aws dynamodb delete-item --table-name "${TABLE}" --key "{\"PK\":{\"S\":\"${RS_PK}\"},\"SK\":{\"S\":\"${REF_SK}\"}}" >/dev/null 2>&1 || true
aws dynamodb delete-item --table-name "${TABLE}" --key "{\"PK\":{\"S\":\"${RH_PK}\"},\"SK\":{\"S\":\"${HIST_SK}\"}}" >/dev/null 2>&1 || true
aws dynamodb delete-item --table-name "${TABLE}" --key "{\"PK\":{\"S\":\"${MS_PK}\"},\"SK\":{\"S\":\"${PACK_SK}\"}}" >/dev/null 2>&1 || true

echo "=== ALL 5 FLOCI WIRE PARITY CHECKS PASSED ==="
