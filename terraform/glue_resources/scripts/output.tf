
output "output_main_runner_script_path"{
  value = "s3://${var.artifact_bucket_name}/${aws_s3_object.main_runner_script.id}"
}

output "output_libraries_path"{
  value = "s3://${var.artifact_bucket_name}/${aws_s3_object.snowfall_pipeline_zip.id}"
}