resource "aws_glue_workflow" "semantic_franchisee_incidents" {
  name = "uk-snowfall-semantic-franchisee-incidents"
  tags = var.resource_tags
  description = "Semantic flow for the franchisee incidents"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "franchisee_incidents"
    "GROUP"                  = "semantic"
    "REPORTING_DATE"         = ""
  }
}


resource "aws_glue_trigger" "semantic_franchisee_incidents_trigger" {
  name = "uk-snowfall-semantic-franchisee-incidents-trigger"
  type = "ON_DEMAND"
  enabled = true
  workflow_name = aws_glue_workflow.semantic_franchisee_incidents.name

  actions {
    job_name = var.glue_job_name
  }

}