resource "aws_sns_topic" "snowfall_topic" {
  name = "uk-snowfall-notification-${var.environment}"  
  display_name = "Snowfall Pipeline Notification" 
  tags = var.resource_tags
}