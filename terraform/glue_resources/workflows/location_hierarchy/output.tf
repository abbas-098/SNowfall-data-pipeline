output "location_hierarchy_workflow_trigger_arn" {
  value = aws_glue_workflow.location_hierarchy.arn
  description = "The workflow trigger function ARN number"
}
