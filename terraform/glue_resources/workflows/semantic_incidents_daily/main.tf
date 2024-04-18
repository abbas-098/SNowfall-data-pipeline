resource "aws_glue_workflow" "semantic_incidents_daily" {
  name = "uk-snowfall-semantic-incidents-daily"
  tags = var.resource_tags
  description = "Semantic flow for the incidents daily"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "daily_incidents"
    "GROUP"                  = "semantic"
    "REPORTING_DATE"         = ""
  }
}


resource "aws_glue_trigger" "semantic_incident_trigger" {
  name = "uk-snowfall-semantic-incidents-trigger"
  type = "ON_DEMAND"
  enabled = true
  workflow_name = aws_glue_workflow.semantic_incidents_daily.name

  actions {
    job_name = var.glue_job_name
  }

}