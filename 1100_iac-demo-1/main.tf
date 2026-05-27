terraform {
  required_providers {
    aws    = { source = "hashicorp/aws" }     # AWS provider for infra
    random = { source = "hashicorp/random" }  # Random provider for unique IDs
  }
}

provider "aws" {
  region = "eu-central-1"   # Target AWS region
}

# Generate random hex suffix (8 chars) for uniqueness
resource "random_id" "suffix" {
  byte_length = 4
}

# Create S3 bucket with unique name + tags
resource "aws_s3_bucket" "demo" {
  bucket = "iac-demo-${random_id.suffix.hex}"  # final name like iac-demo-1a2b3c4d

  tags = {
    Name = "iac-demo"   # project identifier
    Env  = "dev"        # environment tag
  }

  # force_destroy = true   # (optional) auto-delete bucket + objects on destroy
}

# Output the bucket name for reference/automation
output "bucket_name" {
  value = aws_s3_bucket.demo.bucket
}
