# Send notifications to EventBridge for all events in the bucket
# Bucket must exist before attaching a notification, will also 
# target a glue workflow which must too exist before attaching.


data "terraform_remote_state" "core_module" {
  backend = "s3"

  config = {
    bucket = var.terraform_bucket_name
    key     = "snowfall-data-pipeline/core_delta_lake/terraform.tfstate"
    region = "eu-central-1"
  }
}

data "terraform_remote_state" "glue_module" {
  backend = "s3"

  config = {
    bucket = var.terraform_bucket_name
    key     = "snowfall-data-pipeline/glue_resources/terraform.tfstate"
    region = "eu-central-1"
  }
}

resource "aws_cloudwatch_event_rule" "location_event_rule" {
  name = "uk-snowfall-location-trigger-rule"
  description   = "Object create events on bucket s3://${data.terraform_remote_state.core_module.outputs.raw_bucket_name}"
  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail": {
    "bucket": {
      "name": ["${data.terraform_remote_state.core_module.outputs.raw_bucket_name}"]
    },
    "object": {
      "key": [{
        "prefix": "service_now/location/"
      }]
    }
  },
  "detail-type": ["Object Created"]
}
EOF
}

resource "aws_cloudwatch_event_target" "location_rule" {
  rule      = aws_cloudwatch_event_rule.location_event_rule.name
  arn       = data.terraform_remote_state.glue_module.outputs.location_workflow_trigger_arn
  role_arn = var.role_assumed_arn

}