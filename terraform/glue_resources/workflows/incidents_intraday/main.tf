resource "aws_glue_workflow" "incidents_intraday" {
  name = "uk-snowfall-incidents-intraday"
  tags = var.resource_tags
  description = "Workflow for the incidents data that is ran overnight"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "location"
    "GROUP"                  = "preparation"
  }
}


resource "aws_glue_trigger" "incidents_intraday" {
  name = "uk-snowfall-incidents-intraday-workflow-trigger"
  type = "EVENT"
  enabled = true
  workflow_name = aws_glue_workflow.incidents_intraday.name

  actions {
    job_name = var.glue_job_name
  }

  event_batching_condition {
    batch_size = 100
    batch_window = 10
  }
}