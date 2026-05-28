terraform {
  backend "s3" {
    bucket         = "iac-remote-state-bucket-pc"
    key            = "terraform/terraform.tfstate"
    region         = "eu-central-1"
    use_lockfile   = true
    encrypt        = true 
  }

  required_providers {
    aws    = {  # AWS provider for infra
      source  = "hashicorp/aws" 
    }        
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
  bucket_name = "myapp-${terraform.workspace}-bucket-${random_id.suffix.hex}"
  
  tags = {
    Name = "myapp-${terraform.workspace}-bucket-${random_id.suffix.hex}"
    Env  = terraform.workspace
  }
}

output "bucket_id" {
  value = module.mybucket.bucket_id
}
