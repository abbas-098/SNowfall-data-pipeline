output "incident_intraday_workflow_trigger_arn" {
  value = aws_glue_workflow.incident_intraday.arn
  description = "The workflow trigger function ARN number"
}
