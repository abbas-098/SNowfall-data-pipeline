resource "aws_glue_workflow" "incident_daily" {
  name = "uk-snowfall-incident-daily"
  tags = var.resource_tags
  description = "Workflow for the incident-daily data"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "incident_daily"
    "GROUP"                  = "preparation"
  }
}


resource "aws_glue_trigger" "incident_daily" {
  name = "uk-snowfall-incident-daily-trigger"
  type = "EVENT"
  enabled = true
  workflow_name = aws_glue_workflow.incident_daily.name

  actions {
    job_name = var.glue_job_name
  }

  event_batching_condition {
    batch_size = 100
    batch_window = 10
  }
}