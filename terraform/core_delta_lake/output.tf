output "landing_bucket_name" {
  value = module.s3_module_main.landing_bucket_name
  description = "The Name for the landing bucket"
}

output "landing_bucket_arn" {
  value = module.s3_module_main.landing_bucket_arn
  description = "The ARN for the Landing bucket"
}


output "raw_bucket_name" {
  value = module.s3_module_main.raw_bucket_name
  description = "The Name for the Raw bucket"
}


output "raw_bucket_arn" {
  value = module.s3_module_main.raw_bucket_arn
  description = "The ARN for the Raw bucket"
}


output "preparation_bucket_name" {
  value = module.s3_module_main.preparation_bucket_name
  description = "The Name for the Preparation bucket"
}

output "preparation_bucket_bucket_arn" {
  value = module.s3_module_main.preparation_bucket_bucket_arn
  description = "The ARN for the Preparation bucket"
}


output "processed_bucket_name" {
  value = module.s3_module_main.processed_bucket_name
  description = "The Name for the Processed bucket"
}

output "processed_bucket_bucket_arn" {
  value = module.s3_module_main.processed_bucket_bucket_arn
  description = "The ARN for the Processed bucket"
}

output "artifact_bucket_name" {
  value = module.s3_module_main.artifact_bucket_name
  description = "The Name for the Artifact bucket"
}

output "artifact_bucket_bucket_arn" {
  value = module.s3_module_main.artifact_bucket_bucket_arn
  description = "The ARN for the Artifact bucket"
}


output "temporary_folder_path" {
  value = module.s3_module_main.temporary_folder_path
}


output "snowfall_topic_arn" {
  value = module.sns_module.snowfall_topic_arn
}

output "landing_trigger_arn" {
  value = module.lambda_module.landing_trigger_arn
}

