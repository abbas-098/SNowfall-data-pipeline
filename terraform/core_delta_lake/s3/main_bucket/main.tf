####### Creation of Landing Bucket ################

resource "aws_s3_bucket" "landing_bucket" {
  bucket = "eu-central1-${var.environment}-uk-snowfall-landing-${var.account_number}"
  tags = var.resource_tags
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "landing_versioning" {
  bucket = aws_s3_bucket.landing_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_policy" "allow_access_from_appflow_and_connect" {
  bucket = aws_s3_bucket.landing_bucket.id
  policy = data.template_file.bucket_policy.rendered
}

data "template_file" "bucket_policy" {
  template = file("${path.module}/bucket_policy/policy.json")

  vars = {
    environment = var.environment
    account_number = var.account_number
  }
}

resource "aws_s3_object" "landing_folder" {
    bucket 					= "${aws_s3_bucket.landing_bucket.id}"
    acl    					= "private"
    key    					= each.value
    source 					= "/dev/null"
    server_side_encryption 	= "aws:kms"
    for_each = {
      amazon_connect      = "connect/"
      change_request      = "service_now/UK-SNowFall-ServiceNow-ChangeRequest/"
      incident_daily      = "service_now/UK-SNowFall-ServiceNow-Incident-Daily/"
      incident_intraday   = "service_now/UK-SNowFall-ServiceNow-Incident-Intraday/"
      location            = "service_now/UK-SNowFall-ServiceNow-Location/"
      problem_request     = "service_now/UK-SNowFall-ServiceNow-ProblemRequest/"
      service_offering    = "service_now/UK-SNowFall-ServiceNow-ServiceOffering/"
      service_record      = "service_now/UK-SNowFall-ServiceNow-ServiceRecord/"
      sys_user            = "service_now/UK-SNowFall-ServiceNow-SysUser/"
      sys_user_group      = "service_now/UK-SNowFall-ServiceNow-Sys-User-Group/"
      ods                 = "ods/"
    }
}


resource "aws_s3_bucket_notification" "landing_trigger_notification" {
  bucket = aws_s3_bucket.landing_bucket.id

  lambda_function {
    lambda_function_arn = var.lambda_landing_func_arn
    events              = ["s3:ObjectCreated:*"]
    id = "Moving files to Raw Bucket"
  }
  depends_on = [ aws_s3_bucket.landing_bucket,var.lambda_landing_func_arn,var.lambda_permission]
}


# ####### Creation of Raw Bucket ################
resource "aws_s3_bucket" "raw_bucket" {
  bucket = "eu-central1-${var.environment}-uk-snowfall-raw-${var.account_number}"
  tags = var.resource_tags
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "raw_verisoning" {
  bucket = aws_s3_bucket.raw_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "raw_folder" {
    bucket 					= "${aws_s3_bucket.raw_bucket.id}"
    acl    					= "private"
    key    					= each.value
    source 					= "/dev/null"
    server_side_encryption 	= "aws:kms"
    for_each = {
      archive                   = "archive/"
      amazon_connect            = "amazon_connect/"
      change_request            = "service_now/change_request/"
      incident_intraday         = "service_now/incident/intraday/"
      incident_daily            = "service_now/incident/daily/"
      location                  = "service_now/location/"
      problem_request           = "service_now/problem_request/"
      service_record            = "service_now/service_record/"
      service_offering          = "service_now/service_offering/"
      sys_user_group            = "service_now/sys_user_group/"
      sys_user                  = "service_now/sys_user/"
      location_trading_hrs      = "ods/trading_hours/"
      location_hierarchy        = "ods/location_hierarchy/"
      location_adj_trading_hrs  = "ods/adj_trading_hours/"
    }
}

resource "aws_s3_bucket_notification" "enabling_event_bridge_notification" {
  bucket = aws_s3_bucket.raw_bucket.bucket
  eventbridge = true
}


# ####### Creation of Preparation Bucket ################
resource "aws_s3_bucket" "preparation_bucket" {
  bucket = "eu-central1-${var.environment}-uk-snowfall-preparation-${var.account_number}"
  tags = var.resource_tags
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "preparation_verisoning" {
  bucket = aws_s3_bucket.preparation_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "preparation_folder" {
    bucket 					= "${aws_s3_bucket.preparation_bucket.id}"
    acl    					= "private"
    key    					= each.value
    source 					= "/dev/null"
    server_side_encryption 	= "aws:kms"
    for_each = {
      amazon_connect            = "amazon_connect/"
      change_request            = "service_now/change_request/"
      incident_intraday         = "service_now/incident/intraday/"
      incident_daily            = "service_now/incident/daily/"
      location                  = "service_now/location/"
      problem_request           = "service_now/problem_request/"
      service_record            = "service_now/service_record/"
      service_offering          = "service_now/service_offering/"
      sys_user_group            = "service_now/sys_user_group/"
      sys_user                  = "service_now/sys_user/"
      location_trading_hrs      = "ods/trading_hours/"
      location_hierarchy        = "ods/location_hierarchy/"
      location_adj_trading_hrs  = "ods/adj_trading_hours/"
    }
}

# ####### Creation of Artifact Bucket ################

resource "aws_s3_bucket" "artifact_bucket" {
  bucket = "eu-central1-${var.environment}-uk-snowfall-artifact-${var.account_number}"
  tags = var.resource_tags
  force_destroy = true
}


################### Uplaoding Scripts to S3 #######################

resource "aws_s3_object" "temporary_folder" {
  bucket = aws_s3_bucket.artifact_bucket.id
  key    = "temporary/" 
  source = "/dev/null" 
}

#####################################################################################

# ####### Creation of Processed Bucket ################
resource "aws_s3_bucket" "processed_bucket" {
  bucket = "eu-central1-${var.environment}-uk-snowfall-processed-${var.account_number}"
  tags = var.resource_tags
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "processed_verisoning" {
  bucket = aws_s3_bucket.processed_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "processed_folder" {
    bucket 					= "${aws_s3_bucket.processed_bucket.id}"
    acl    					= "private"
    key    					= each.value
    source 					= "/dev/null"
    server_side_encryption 	= "aws:kms"
    for_each = {
      amazon_connect            = "amazon_connect/"
      change_request            = "service_now/change_request/"
      incident_intraday         = "service_now/incident/intraday/"
      incident_daily            = "service_now/incident/daily/"
      location                  = "service_now/location/"
      problem_request           = "service_now/problem_request/"
      service_record            = "service_now/service_record/"
      service_offering          = "service_now/service_offering/"
      sys_user_group            = "service_now/sys_user_group/"
      sys_user                  = "service_now/sys_user/"
      location_trading_hrs      = "ods/trading_hours/"
      location_hierarchy        = "ods/location_hierarchy/"
      location_adj_trading_hrs  = "ods/adj_trading_hours/"
    }
}



# ####### Creation of Semantic Bucket ################
resource "aws_s3_bucket" "semantic_bucket" {
  bucket = "eu-central1-${var.environment}-uk-snowfall-semantic-${var.account_number}"
  tags = var.resource_tags
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "semantic_verisoning" {
  bucket = aws_s3_bucket.semantic_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

###### Creation of Athena Bucket ##############
resource "aws_s3_bucket" "athena_bucket" {
  bucket = "eu-central1-${var.environment}-uk-snowfall-athena-${var.account_number}"
  tags   = var.resource_tags
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_lifecycle" {
  bucket = aws_s3_bucket.athena_bucket.id

  rule {
    id     = "Delete After 30 Days"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}

