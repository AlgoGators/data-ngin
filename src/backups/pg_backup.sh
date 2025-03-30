#!/bin/bash

# Backup PostgreSQL database and upload to S3
TIMESTAMP=$(date +"%F-%H%M")
DB_HOST= DB_HOST
DB_NAME= DB_NAME
DB_USER= DB_USER
FILENAME="${DB_NAME}_${TIMESTAMP}.sql.gz"
TMP_PATH="/tmp/${FILENAME}"
S3_BUCKET="s3-bucket-name"
S3_PATH="s3://${S3_BUCKET}/postgres-backups/${FILENAME}"

pg_dump -h "$DB_HOST" -U "$DB_USER" "$DB_NAME" | gzip > "$TMP_PATH"
aws s3 cp "$TMP_PATH" "$S3_PATH"
rm "$TMP_PATH"
