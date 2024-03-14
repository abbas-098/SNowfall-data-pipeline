output "landing_trigger_arn" {
  value = aws_lambda_function.uk_snowfall_landing_function.arn
  description = "The landing trigger function ARN number"
}

output "lambda_s3_permission" {
  value = aws_lambda_permission.allow_landing_bucket.id
  description = "The permission for lambda to accept s3 events"
}