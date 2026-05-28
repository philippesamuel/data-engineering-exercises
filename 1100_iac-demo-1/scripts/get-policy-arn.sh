#!/usr/bin/bash
# 'AmazonS3FullAccess'
# 'AmazonDynamoDBFullAccess' 
aws iam list-policies --query="Policies[?PolicyName == 'AmazonS3FullAccess']" --profile root | jq ".[0].Arn"
