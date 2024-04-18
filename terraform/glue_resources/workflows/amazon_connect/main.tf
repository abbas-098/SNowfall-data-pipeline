resource "aws_glue_workflow" "amazon_connect" {
  name = "uk-snowfall-amazon-connect"
  tags = var.resource_tags
  description = "Workflow for the amazon_connect data"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "amazon_connect"
    "GROUP"                  = "preparation"
  }
}


resource "aws_glue_trigger" "amazon_connect" {
  name = "uk-snowfall-amazon-connect-trigger"
  type = "EVENT"
  enabled = true
  workflow_name = aws_glue_workflow.amazon_connect.name

  actions {
    job_name = var.glue_job_name
  }

  event_batching_condition {
    batch_size = 100
    batch_window = 10
  }
}