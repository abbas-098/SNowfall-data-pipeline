terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.33.0"
    }
  }

  backend "s3" {
    key     = "snowfall-data-pipeline/other_resources/terraform.tfstate"
    region  = "eu-central-1"
    encrypt = true
  }
}

#Picks up from secrets in github
provider "aws" {
  region      = var.AWS_REGION
}

  # #Picks up from secrets in github
  # provider "aws" {
  #   region      = var.AWS_REGION
  #   access_key = ""
  #   secret_key = "1"
  #   token = ""
  # }


# Triggering event bridge module
module "event_bridge_module" {
  source        = "./eventbridge"
  environment   = var.environment
  role_assumed_arn = var.role_assumed_arn
  account_number = var.account_number
  resource_tags = merge(var.resource_tags, { Environment = var.environment })
  terraform_bucket_name   = var.terraform_bucket_name
}
