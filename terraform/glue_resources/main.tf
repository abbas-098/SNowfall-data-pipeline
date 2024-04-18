terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.33.0"
    }
  }

  backend "s3" {
    key     = "snowfall-data-pipeline/glue_resources/terraform.tfstate"
    region  = "eu-central-1"
    encrypt = true
  }
}


data "terraform_remote_state" "core_module" {
  backend = "s3"
  config = {
    bucket = var.terraform_bucket_name
    key    = "snowfall-data-pipeline/core_delta_lake/terraform.tfstate"
    region = "eu-central-1"
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
  


# Triggering the Scripts module
module "scripts_module" {
  source = "./scripts"
  artifact_bucket_name = data.terraform_remote_state.core_module.outputs.artifact_bucket_name

}



#### Creating the main runner script that will pass into each module ######

resource "aws_glue_job" "main_runner_script" {
  name     = "uk-snowfall-main-script-runner"
  role_arn = var.role_assumed_arn
  tags = var.resource_tags
  glue_version = "4.0"
  max_capacity = 2
  timeout = 120
  description = "Main driver script for all pipelines"


  command {
    script_location = module.scripts_module.output_main_runner_script_path
    python_version = 3
       
  }
  default_arguments = {
    
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--datalake-formats"                 = "delta"
    "--enable-auto-scaling"              = "true"
    "--conf"                             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--SNS_TOPIC_ARN"                    = data.terraform_remote_state.core_module.outputs.snowfall_topic_arn
    "--TempDir"                          = data.terraform_remote_state.core_module.outputs.temporary_folder_path
    "--extra-py-files"                   = module.scripts_module.output_libraries_path
    "--ACCOUNT_NUMBER"                   = var.account_number
    "--ENVIRONMENT"                      = var.environment
  }

  execution_property {
    max_concurrent_runs = 100
  }
}
################## Triggering Workflows #######################################

# Triggering the location
module "location_module" {
  source = "./workflows/location"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}

# Triggering the amazon_connect
module "amazon_connect_module" {
  source = "./workflows/amazon_connect"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}

# Triggering the incident_intraday
module "incident_intraday_module" {
  source = "./workflows/incident_intraday"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}

# Triggering the incident_daily
module "incident_daily_module" {
  source = "./workflows/incident_daily"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}

# Triggering the location_hierarchy
module "location_hierarchy_module" {
  source = "./workflows/location_hierarchy"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}

# Triggering the semantic_incidents_daily
module "semantic_incidents_daily_module" {
  source = "./workflows/semantic_incidents_daily"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}

# Triggering the semantic_franchisee_incidents
module "semantic_franchisee_incidents_module" {
  source = "./workflows/semantic_franchisee_incidents"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}

# Triggering the semantic_amazon_connect
module "semantic_amazon_connect_module" {
  source = "./workflows/semantic_amazon_connect"
  resource_tags = merge(var.resource_tags,{Environment = var.environment})
  glue_job_name = aws_glue_job.main_runner_script.name

}


############ Glue Data Catalog Databases #########
resource "aws_glue_catalog_database" "preparation_database" {
  name = "uk_snowfall_preparation"
  description = "Datasets that have been cleansed and validated and remain at the same level they were initally sourced"

  lifecycle {
    ignore_changes = [name,description]
    prevent_destroy = false  # Allow Terraform to delete the database
  }
}

resource "aws_glue_catalog_database" "processed_database" {
  name = "uk_snowfall_processed"
  description = "Datasets that have been transformed and enriched for ease of use"

  lifecycle {
    ignore_changes = [name,description]
    prevent_destroy = false  # Allow Terraform to delete the database
  }
}

resource "aws_glue_catalog_database" "semantic_database" {
  name = "uk_snowfall_semantic"
  description = "Datasets that have been aggregated and made available for reporting and analytics, with buisness logic built in"

  lifecycle {
    ignore_changes = [name,description]
    prevent_destroy = false  # Allow Terraform to delete the database
  }
}

resource "aws_glue_catalog_database" "microstrategy_database" {
  name = "uk_snowfall_microstrategy"
  description = "Playground for MicroStrategy"

  lifecycle {
    ignore_changes = [name,description]
    prevent_destroy = false  # Allow Terraform to delete the database
  }
}