data "archive_file" "snowfall_pipeline_zip" {
  type        = "zip"
  source_dir = "${path.module}/python/"
  output_path = "${path.module}/zips/snowfall_pipeline.zip"
}


################### Uplaoding Scripts to S3 #######################


resource "aws_s3_object" "main_runner_script" {
  bucket = var.artifact_bucket_name
  key    = "glue_script/main_runner.py" 
  source = "${path.module}/main_runner.py" 
  etag = filemd5("${path.module}/main_runner.py")
}


########### Uploading Libraries ######################

resource "aws_s3_object" "snowfall_pipeline_zip" {
  bucket = var.artifact_bucket_name
  key    = "glue_libraries/snowfall_pipeline.zip" 
  source = "${path.module}/zips/snowfall_pipeline.zip" 
  etag = filemd5("${path.module}/zips/snowfall_pipeline.zip")
}

