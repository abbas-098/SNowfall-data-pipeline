output "amazon_connect_workflow_trigger_arn" {
  value = aws_glue_workflow.amazon_connect.arn
  description = "The workflow trigger function ARN number"
}
