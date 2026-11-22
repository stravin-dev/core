#!/bin/sh
set -eu

export AWS_PAGER=""
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://floci-region-a:4566}"

BUCKET="${STRAVIN_S3_BUCKET:-git-objects-${AWS_DEFAULT_REGION}}"
TABLE="${STRAVIN_DDB_TABLE:-stravin-core}"
KMS_ALIAS="${STRAVIN_KMS_ALIAS:-alias/stravin-local-dev}"
QUEUE_NAME="${FLOCI_SYNC_QUEUE_NAME:-floci-test-s3-sync-queue}"
SEED_DEFAULT_REPO="${STRAVIN_SEED_DEFAULT_REPO:-0}"
TENANT="${STRAVIN_DEFAULT_TENANT:-local}"
REPO="${STRAVIN_DEFAULT_REPO:-demo}"

echo "[floci-init-mr] Bootstrapping Floci at ${AWS_ENDPOINT_URL} (region=${AWS_DEFAULT_REGION})"

# 1. Regional S3 bucket (idempotent)
if aws s3api head-bucket --bucket "${BUCKET}" >/dev/null 2>&1; then
  echo "[floci-init-mr] S3 bucket '${BUCKET}' already exists"
else
  if [ "${AWS_DEFAULT_REGION}" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "${BUCKET}" >/dev/null
  else
    aws s3api create-bucket --bucket "${BUCKET}" \
      --create-bucket-configuration "LocationConstraint=${AWS_DEFAULT_REGION}" >/dev/null
  fi
  echo "[floci-init-mr] Created S3 bucket '${BUCKET}'"
fi

# 2. DynamoDB table with PK/SK and NEW_IMAGE StreamSpecification (idempotent)
if aws dynamodb describe-table --table-name "${TABLE}" >/dev/null 2>&1; then
  echo "[floci-init-mr] DynamoDB table '${TABLE}' already exists"
else
  aws dynamodb create-table \
    --table-name "${TABLE}" \
    --attribute-definitions \
      AttributeName=PK,AttributeType=S \
      AttributeName=SK,AttributeType=S \
    --key-schema \
      AttributeName=PK,KeyType=HASH \
      AttributeName=SK,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE \
    >/dev/null
  aws dynamodb wait table-exists --table-name "${TABLE}"
  echo "[floci-init-mr] Created DynamoDB table '${TABLE}' (StreamViewType=NEW_IMAGE)"
fi

# 3. DynamoDB TTL on 'ttl' attribute (idempotent)
TTL_STATUS=$(aws dynamodb describe-time-to-live \
  --table-name "${TABLE}" \
  --query 'TimeToLiveDescription.TimeToLiveStatus' \
  --output text 2>/dev/null || echo "DISABLED")

if [ "${TTL_STATUS}" = "ENABLED" ] || [ "${TTL_STATUS}" = "ENABLING" ]; then
  echo "[floci-init-mr] DynamoDB TTL on '${TABLE}.ttl' already ${TTL_STATUS}"
else
  aws dynamodb update-time-to-live \
    --table-name "${TABLE}" \
    --time-to-live-specification "Enabled=true,AttributeName=ttl" \
    >/dev/null
  echo "[floci-init-mr] Enabled DynamoDB TTL on '${TABLE}.ttl'"
fi

# 4. KMS symmetric key + alias/stravin-local-dev (idempotent)
if aws kms describe-key --key-id "${KMS_ALIAS}" >/dev/null 2>&1; then
  echo "[floci-init-mr] KMS alias '${KMS_ALIAS}' already exists"
else
  KEY_ID=$(aws kms create-key \
    --description "Stravin local multi-region symmetric CMK (${AWS_DEFAULT_REGION})" \
    --key-usage ENCRYPT_DECRYPT \
    --key-spec SYMMETRIC_DEFAULT \
    --query 'KeyMetadata.KeyId' \
    --output text)
  aws kms create-alias \
    --alias-name "${KMS_ALIAS}" \
    --target-key-id "${KEY_ID}"
  echo "[floci-init-mr] Created KMS key '${KEY_ID}' with alias '${KMS_ALIAS}'"
fi

# 5. SQS queue for S3 event notifications (idempotent)
QUEUE_URL=$(aws sqs create-queue \
  --queue-name "${QUEUE_NAME}" \
  --query 'QueueUrl' \
  --output text)
QUEUE_ARN=$(aws sqs get-queue-attributes \
  --queue-url "${QUEUE_URL}" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text 2>/dev/null || echo "arn:aws:sqs:${AWS_DEFAULT_REGION}:000000000000:${QUEUE_NAME}")
if [ -z "${QUEUE_ARN}" ] || [ "${QUEUE_ARN}" = "None" ]; then
  QUEUE_ARN="arn:aws:sqs:${AWS_DEFAULT_REGION}:000000000000:${QUEUE_NAME}"
fi
echo "[floci-init-mr] SQS queue '${QUEUE_NAME}' ready (arn=${QUEUE_ARN})"

# 6. S3 PutBucketNotificationConfiguration forwarding ObjectCreated & ObjectRemoved to SQS
aws s3api put-bucket-notification-configuration \
  --bucket "${BUCKET}" \
  --notification-configuration "{
    \"QueueConfigurations\": [
      {
        \"Id\": \"floci-test-s3-sync\",
        \"QueueArn\": \"${QUEUE_ARN}\",
        \"Events\": [\"s3:ObjectCreated:*\", \"s3:ObjectRemoved:*\"]
      }
    ]
  }"
echo "[floci-init-mr] Configured S3->SQS notifications on '${BUCKET}' -> '${QUEUE_ARN}'"

# 7. Optional default repo META seed (enabled on Region A only so ddb-sync replicates cleanly)
if [ "${SEED_DEFAULT_REPO}" = "1" ]; then
  META_PK="T#${TENANT}#REPO#${REPO}"
  if aws dynamodb put-item \
    --table-name "${TABLE}" \
    --item "{
      \"PK\": {\"S\": \"${META_PK}\"},
      \"SK\": {\"S\": \"META\"},
      \"home_region\": {\"S\": \"${AWS_DEFAULT_REGION}\"},
      \"failover_epoch\": {\"N\": \"1\"},
      \"ref_shards\": {\"N\": \"1\"},
      \"manifest_shards\": {\"N\": \"1\"},
      \"write_state\": {\"S\": \"ACTIVE\"}
    }" \
    --condition-expression "attribute_not_exists(PK)" >/dev/null 2>&1; then
    echo "[floci-init-mr] Seeded default repo META item (${META_PK})"
  else
    echo "[floci-init-mr] Default repo META item (${META_PK}) already exists"
  fi
fi

echo "[floci-init-mr] Bootstrap complete (${AWS_DEFAULT_REGION})."
