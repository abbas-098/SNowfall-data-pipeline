resource "aws_glue_workflow" "location" {
  name = "uk-snowfall-location"
  tags = var.resource_tags
  description = "Workflow for the location data"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "location"
    "GROUP"                  = "preparation"
  }
}


resource "aws_glue_trigger" "location" {
  name = "uk-snowfall-location-trigger"
  type = "EVENT"
  enabled = true
  workflow_name = aws_glue_workflow.location.name

  actions {
    job_name = var.glue_job_name
  }

  event_batching_condition {
    batch_size = 100
    batch_window = 10
  }
}