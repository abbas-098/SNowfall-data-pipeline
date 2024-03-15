resource "null_resource" "create_appflow" {
    count = 1
    provisioner "local-exec" {
        command = <<-EOT
            cd appflow &&
            python3 create_appflow.py
        EOT
        
        environment = {
          LandingBucket = var.landing_bucket_name
          ConnectorProfileName = var.connector_profile_name
        }
    }
}


resource "null_resource" "delete_appflow" {
    count = 1
    provisioner "local-exec" {
        command = <<-EOT
            cd appflow &&
            python3 delete_appflow.py
        EOT
        when = destroy
    }
}