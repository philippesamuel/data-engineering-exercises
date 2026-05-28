#!/usr/bin/bash
# S3 bucket for state
aws s3api create-bucket --bucket iac-remote-state-bucket-pc --region eu-central-1

# Enable encryption
aws s3api put-bucket-encryption --bucket iac-remote-state-bucket-pc \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
