module "incident_daily" {
    source = "./incident_daily"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags

}

module "incident_intraday" {
    source = "./incident_intraday"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}

module "change_request" {
    source = "./change_request"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}

module "location" {
    source = "./location"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}

module "problem_request" {
    source = "./problem_request"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}

module "service_offering" {
    source = "./service_offering"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}

module "service_request" {
    source = "./service_request"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}

module "sys_user" {
    source = "./sys_user"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}

module "sys_user_group" {
    source = "./sys_user_group"  
    landing_bucket_name = var.landing_bucket_name
    connector_profile_name = var.connector_profile_name
    resource_tags = var.resource_tags
}