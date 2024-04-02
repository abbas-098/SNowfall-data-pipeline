output "location_workflow_trigger_arn" {
  value = aws_glue_workflow.location.arn
  description = "The workflow trigger function ARN number"
}
