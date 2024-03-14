terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.33.0"
    }
  }

  backend "s3" {
    key     = "snowfall-data-pipeline/core_delta_lake/terraform.tfstate"
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
  

# Triggering the S3 Module
module "s3_module_main" {
  source                  = "./s3/main_bucket"
  environment             = var.environment
  account_number          = var.account_number
  resource_tags           = merge(var.resource_tags, { Environment = var.environment,DataClassification = "highly restricted" })
  role_assumed_arn        = var.role_assumed_arn
  lambda_landing_func_arn = module.lambda_module.landing_trigger_arn
  lambda_permission       = module.lambda_module.lambda_s3_permission
}

# Triggering the Lambda Module
module "lambda_module" {
  source             = "./lambda"
  environment        = var.environment
  resource_tags      = merge(var.resource_tags, { Environment = var.environment })
  role_assumed_arn   = var.role_assumed_arn
  landing_bucket_arn = module.s3_module_main.landing_bucket_arn
  sns_topic_arn      = module.sns_module.snowfall_topic_arn
  account_number = var.account_number
}

# Triggering the SNS Module. Will have to change to fix endpoint as email
module "sns_module" {
  source        = "./sns"
  environment   = var.environment
  resource_tags = merge(var.resource_tags, { Environment = var.environment })
}

module "appflow_module" {
  source        = "./appflow"
  environment             = var.environment
  resource_tags           = merge(var.resource_tags, { Environment = var.environment })
  role_assumed_arn        = var.role_assumed_arn
  landing_bucket_arn      = module.s3_module_main.landing_bucket_arn
  sns_topic_arn           = module.sns_module.snowfall_topic_arn
  account_number          = var.account_number
  connector_profile_name  = var.connector_profile_name
  landing_bucket_name     = module.s3_module_main.landing_bucket_name
}



