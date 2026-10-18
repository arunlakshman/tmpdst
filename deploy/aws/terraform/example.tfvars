# cp example.tfvars my.tfvars (my.tfvars is git-ignored), then:
#   terraform apply -var-file=my.tfvars
region       = "us-west-2"
az           = "us-west-2a"
run_id       = "r1"
owner        = "you@example.com"
budget_usd   = 50
budget_email = "you@example.com"
