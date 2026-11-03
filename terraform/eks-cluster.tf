module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8.2"

  cluster_name    = "nexus-cluster"
  cluster_version = "1.30"

  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    region_a_high_cpu = {
      instance_types = ["t3.small"]
      ami_type       = "AL2_x86_64"
      min_size     = 3
      max_size     = 5
      desired_size = 3
      labels = {
        region   = "region-a"
        cpu-tier = "high"
      }
    }
    region_b_standard = {
      instance_types = ["t3.small"]
      ami_type       = "AL2_x86_64"
      min_size     = 2
      max_size     = 5
      desired_size = 2
      labels = {
        region   = "region-b"
        cpu-tier = "standard"
      }
    }
  }

  # Cost-saving fix: Enable Prefix Delegation to allow 110 pods per t3.small
  cluster_addons = {
    vpc-cni = {
      before_compute = true
      most_recent    = true
      configuration_values = jsonencode({
        env = {
          ENABLE_PREFIX_DELEGATION = "true"
          WARM_PREFIX_TARGET       = "1"
        }
      })
    }
  }

  enable_cluster_creator_admin_permissions = true
}

