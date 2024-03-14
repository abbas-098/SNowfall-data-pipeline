output "snowfall_topic_arn" {
  value = aws_sns_topic.snowfall_topic.arn
  description = "The ARN of the SNS topic created"
}