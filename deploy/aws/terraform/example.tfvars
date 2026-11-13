# cp example.tfvars my.tfvars (my.tfvars is git-ignored), then:
#   terraform apply -var-file=my.tfvars
# Defaults build the WarpStream-comparable stack: 6 x m6in.4xlarge brokers,
# 2 x m6in.8xlarge load generators, internal NLB, DynamoDB warm throughput 30k/30k.
# Fixed cost about $11.8/hour (see README); up to about $40/hour at full 1 GiB/s load
# (DynamoDB commits dominate). 4 h: about $47 fixed, about $160 worst case.
region       = "ap-south-2"
az           = "ap-south-2a"
run_id       = "r1"
owner        = "you@example.com"
budget_usd   = 200 # monthly, account-wide (80% actual alert = $160)
budget_email = "you@example.com"

# broker_count        = 6
# broker_instance_type  = "m6in.4xlarge"
# loadgen_count       = 2
# loadgen_instance_type = "m6in.8xlarge"
# use_nlb             = true
# max_runtime_minutes = 360
