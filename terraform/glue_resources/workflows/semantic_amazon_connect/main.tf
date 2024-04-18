resource "aws_glue_workflow" "semantic_amazon_connect" {
  name = "uk-snowfall-semantic-amazon-connect"
  tags = var.resource_tags
  description = "Semantic flow for the amazon connect"
  max_concurrent_runs = 1
  default_run_properties = {

    "DATASET"                = "amazon_connect"
    "GROUP"                  = "semantic"
    "REPORTING_DATE"         = ""
  }
}


resource "aws_glue_trigger" "semantic_amazon_connect_trigger" {
  name = "uk-snowfall-semantic-amazon-connect-trigger"
  type = "ON_DEMAND"
  enabled = true
  workflow_name = aws_glue_workflow.semantic_amazon_connect.name

  actions {
    job_name = var.glue_job_name
  }

}