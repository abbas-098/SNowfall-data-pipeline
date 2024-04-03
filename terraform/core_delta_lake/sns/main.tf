resource "aws_sns_topic" "snowfall_topic" {
  name = "uk-snowfall-notification-${var.environment}"  
  display_name = "Snowfall Pipeline Notification" 
  tags = var.resource_tags
}

resource "aws_sns_topic_subscription" "email-target" {
  topic_arn = aws_sns_topic.snowfall_topic.arn
  protocol  = "email"
  endpoint  = "abbas-97@outlook.com"
}

