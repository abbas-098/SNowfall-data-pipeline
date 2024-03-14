variable "resource_tags" {
  type        = map(string)
  description = "Tagging of all resources created using Terraform"

  default = {
    Application_ID  = "APP2002212"
    Owner           = "andrew.platts@uk.mcd.com"
    GBL             = "195500433387"
    Market          = "GB"
    Application     = "SNOW"
    Purpose         = "SNOWFALL"
    Budget_Owner    = "andrew.platts@uk.mcd.com"
    IT_Owner        = "andrew.platts@uk.mcd.com"


  }
}

variable "environment" {
  type        = string
  description = "The different enviornment this code is deployed"
  default     = "development"
}

variable "account_number" {
  type        = string
  description = "AWS Account Number"
  default     = ""
}

variable "role_assumed_arn" {
  type        = string
  description = "The ARN of the role which is to be assumed. We assume it is already created by AMS Teams"
  nullable    = false
  default     = ""
}


variable "AWS_REGION" {
  description = "AWS region"
  default = "eu-central-1"
  
}

variable "terraform_bucket_name" {

  description = "Bucket Name for where terraform state file is stored"
  default = "eu-central1-nprod-uk-snowfall-terraform-404060908217"
  
}