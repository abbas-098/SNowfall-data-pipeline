output "incident_intraday_workflow_trigger_arn" {
  value = aws_glue_workflow.incidents_intraday.arn
  description = "The workflow trigger function ARN number"
}
