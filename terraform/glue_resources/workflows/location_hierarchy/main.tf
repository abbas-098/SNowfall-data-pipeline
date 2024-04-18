resource "aws_glue_workflow" "location_hierarchy" {
  name = "uk-snowfall-location-hierarchy"
  tags = var.resource_tags
  description = "Workflow for the location-hierarchy data"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "location_hierarchy"
    "GROUP"                  = "preparation"
  }
}


resource "aws_glue_trigger" "location_hierarchy" {
  name = "uk-snowfall-location-hierarchy-trigger"
  type = "EVENT"
  enabled = true
  workflow_name = aws_glue_workflow.location_hierarchy.name

  actions {
    job_name = var.glue_job_name
  }

  event_batching_condition {
    batch_size = 100
    batch_window = 10
  }
}