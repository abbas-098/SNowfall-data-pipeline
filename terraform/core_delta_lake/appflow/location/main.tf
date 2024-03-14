provider "aws" {
  region = "eu-central-1"
}

resource "aws_appflow_flow" "location_flow" {
  name = "UK-SNowFall-ServiceNow-Location"
  tags = var.resource_tags

  source_flow_config {
    connector_type = "Servicenow"
    connector_profile_name = var.connector_profile_name
    source_connector_properties {
        service_now {
          object = "cmn_location"
        }
    }
  }

  destination_flow_config {
    connector_type = "S3"
    destination_connector_properties {
      s3 {
        bucket_name = var.landing_bucket_name
        bucket_prefix = "service_now"


        s3_output_format_config {
            aggregation_config {
              aggregation_type = "SingleFile"
            }
            prefix_config {
              prefix_format = "DAY"
              prefix_type = "PATH_AND_FILENAME"
            }
            file_type = "JSON"
        }
      }
    }
  }

  task {
    source_fields     = [""]
    task_type         = "Map_all"
    
  }

  trigger_config {
    trigger_type = "Scheduled"
    trigger_properties {
      scheduled {
        schedule_expression = "cron(0 1 ? * MON-SUN *)"
        data_pull_mode      = "Incremental"

        first_execution_from = null
        timezone = "GMT"
      }
    
    }
  }
}
