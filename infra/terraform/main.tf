terraform {
  required_version = ">= 1.6"
}

provider "aws" {
  region = var.aws_region
}
