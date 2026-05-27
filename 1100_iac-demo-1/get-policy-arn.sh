#!/usr/bin/bash
aws iam list-policies --query="Policies[?PolicyName == 'AmazonS3FullAccess']" --profile root | jq ".[0].Arn"
