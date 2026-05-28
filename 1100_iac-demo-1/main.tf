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

module "mybucket" {
  source      = "./modules/s3_bucket"
  bucket_name = "terraform-module-bucket-${random_id.suffix.hex}"
  tags = {
    owner = "philippe.sa.costa@gmail.com"
    env   = "dev"
  }
}

output "bucket_id" {
  value = module.mybucket.bucket_id
}
