output "incident_daily_workflow_trigger_arn" {
  value = aws_glue_workflow.incident_daily.arn
  description = "The workflow trigger function ARN number"
}
