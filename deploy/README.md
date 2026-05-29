# Deployment

This project runs as a batch workload, so the deploy target is a Kubernetes
`CronJob` rather than a long-running `Deployment`.

## Structure

```text
Dockerfile
.dockerignore
.github/workflows/deploy.yml
deploy/kubernetes/base/
deploy/kubernetes/overlays/dev/
deploy/kubernetes/overlays/prod/
infra/terraform/ecr.tf
infra/terraform/iam.tf
```

## Recommended Flow

1. Build the Python pipeline into an immutable Docker image.
2. Push the image to Amazon ECR with a unique workflow tag.
3. Deploy the relevant Kubernetes overlay: `dev` or `prod`.
4. Update the `risk-pipeline-live` CronJob image to the immutable tag.
5. Optionally create a one-off Job from the CronJob for an immediate run.

The deploy workflow is manual by design. GitHub Environments should control who
can deploy to `dev` and `prod`, with required reviewers for production.

## Required GitHub Environment Variables

Create these variables in each GitHub Environment:

```text
AWS_REGION
AWS_ROLE_TO_ASSUME
ECR_REPOSITORY_URI
EKS_CLUSTER_NAME
```

Use separate AWS roles for `dev` and `prod`. The Terraform deploy-role scaffold
trusts GitHub OIDC subjects scoped to `repo:<owner>/<repo>:environment:<name>`.

## Access Controls

Access controls should be layered:

1. GitHub Environment protection rules for deployment approval.
2. GitHub OIDC to AWS IAM roles instead of long-lived AWS keys.
3. ECR immutable tags and image scanning on push.
4. Separate Kubernetes namespaces for `dev` and `prod`.
5. A dedicated `risk-pipeline` Kubernetes service account per namespace.
6. No Kubernetes RBAC grants for the runtime service account unless the app
   needs Kubernetes API access.
7. A deny-ingress NetworkPolicy because this workload does not serve traffic.
8. Pod security settings: non-root user, dropped capabilities, read-only root
   filesystem, explicit CPU and memory requests/limits.

For EKS, map the GitHub deploy role to Kubernetes permissions with EKS access
entries or Kubernetes RBAC. The runtime service account annotations in the
overlays are placeholders and should be replaced with real environment-specific
IAM role ARNs.

## Commands

Build locally:

```bash
docker build -t financial-risk-data-platform:local .
```

Render or apply an overlay:

```bash
kubectl apply -k deploy/kubernetes/overlays/dev
kubectl apply -k deploy/kubernetes/overlays/prod
```

Run the deploy workflow:

```bash
gh workflow run deploy.yml -f environment=dev -f run_now=true
```

## References

- GitHub OIDC for AWS: https://docs.github.com/en/actions/how-tos/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services
- GitHub deployment environments: https://docs.github.com/en/actions/reference/deployments-and-environments
- Docker builds in GitHub Actions: https://docs.docker.com/build/ci/github-actions/
- Kubernetes CronJobs: https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
- EKS access entries: https://docs.aws.amazon.com/eks/latest/userguide/access-entries.html
