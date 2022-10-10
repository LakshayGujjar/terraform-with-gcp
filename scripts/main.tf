resource "google_storage_bucket" "static-site" {
  name          = "image-store01-images-jenkins"
  location      = "US"
  force_destroy = true

  uniform_bucket_level_access = true
}



# resource "google_compute_instance" "vm_instance" {
# 	name  = "final-time"
# 	machine_type = "f1-micro"
# 	zone = "us-central1-a"

# 	boot_disk {
# 		initialize_params {
# 			# image can be a particular  machine image or a fully qualified image-family name
# 			image = "debian-cloud/debian-9"
# 			# image = "projects/<PROJECT_NAME>/global/images/family/<IMAGE_NAME>"
# 		}
# 		# CMEK (keys managed in Google cloud KMS) 
# 		# kms_key_self_link = "projects/<PROJECT_NAME>/locations/<LOCATION>/keyRings/<EXISTING_PROJ_NAME>/cryptoKeys/<KEY_NAME>"
# 	}

# 	network_interface {
# 		# subnetwork = "projects/<PROJECT_NAME>/regions/<REGION_OF_SUBNET>/<SUBNET_NAME>"
# 		# network_ip = "10.0.0.1" -- Static IP (Private IP address)
# 		network = "default"
#     		access_config {
#     		}
# 	}
# 	scheduling {
# 		on_host_maintenance = "MIGRATE"
# 	}
	
# 	#metadata_startup_script_url = "gs://<BUCKET_NAME>/path/to/file/filename.sh"

#   	service_account {
#     		email  = "terraform-deployer@revanth-gcp-2020.iam.gserviceaccount.com"
#     		scopes = ["cloud-platform"]
#   	}
# 	tags = ["allow-nginx-ingress"]
# }

# output "gce_ext_ip" {
# 	value = google_compute_instance.vm_instance.network_interface[0].access_config[0].nat_ip
# }
