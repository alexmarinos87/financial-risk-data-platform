locals {
  create_github_deploy_roles = var.github_oidc_provider_arn != ""
  eks_cluster_resources      = var.eks_cluster_arn == "" ? ["*"] : [var.eks_cluster_arn]
}

resource "aws_iam_role" "github_deploy" {
  for_each = local.create_github_deploy_roles ? var.github_deploy_environments : toset([])

  name = "${var.project_name}-${each.key}-github-deploy"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = var.github_oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:environment:${each.key}"
          }
        }
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = each.key
  }
}

resource "aws_iam_policy" "github_deploy" {
  for_each = aws_iam_role.github_deploy

  name = "${var.project_name}-${each.key}-github-deploy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:CompleteLayerUpload",
          "ecr:DescribeRepositories",
          "ecr:InitiateLayerUpload",
          "ecr:PutImage",
          "ecr:UploadLayerPart"
        ]
        Resource = aws_ecr_repository.pipeline.arn
      },
      {
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster"
        ]
        Resource = local.eks_cluster_resources
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_deploy" {
  for_each = aws_iam_role.github_deploy

  role       = each.value.name
  policy_arn = aws_iam_policy.github_deploy[each.key].arn
}

output "github_deploy_role_arns" {
  value = {
    for environment, role in aws_iam_role.github_deploy : environment => role.arn
  }
}
