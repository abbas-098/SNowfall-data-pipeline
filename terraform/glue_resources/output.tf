output "location_workflow_trigger_arn" {
  value = module.location_module.location_workflow_trigger_arn
}

output "amazon_connect_workflow_trigger_arn" {
  value = module.amazon_connect_module.amazon_connect_workflow_trigger_arn
}

output "incident_intraday_workflow_trigger_arn" {
  value = module.incident_intraday_module.incident_intraday_workflow_trigger_arn
}

output "location_hierarchy_workflow_trigger_arn" {
  value = module.location_hierarchy_module.location_hierarchy_workflow_trigger_arn
}

output "incident_daily_workflow_trigger_arn" {
  value = module.incident_daily_module.incident_daily_workflow_trigger_arn
}