terraform {
  backend "s3" {
    bucket         = "iac-remote-state-bucket-pc"
    key            = "env/dev/terraform.tfstate"
    region         = "eu-central-1"
    use_lockfile   = true
    encrypt        = true
  }
}

provider "aws" {
  region = "eu-central-1"
}

resource "random_id" "suffix" {
  byte_length = 4
}

module "app_bucket" {
  source           = "../../modules/s3_bucket"
  bucket_name      = "myapp-${terraform.workspace}-bucket-${random_id.suffix.hex}"
  enable_versioning = false
  tags = { env = terraform.workspace }
}
