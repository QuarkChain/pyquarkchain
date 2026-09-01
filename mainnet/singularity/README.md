# Singularity Docker Image

Run the following commands from the repository root.

## Build from the public repository

The default repository is `https://github.com/QuarkChain/pyquarkchain.git`, and
the default revision is `master`:

```bash
docker build \
  -f mainnet/singularity/Dockerfile \
  -t pyquarkchain:master \
  .
```

## Build from a private repository

Create a GitHub personal access token with read access to the target repository.
For a fine-grained token, grant `Contents: Read-only` permission. If the
organization uses SSO, authorize the token for that organization as well.

Provide the token as a BuildKit secret so it is not stored in an image layer or
passed as a Docker build argument:

```bash
read -rsp "GitHub token: " GITHUB_TOKEN
echo
export GITHUB_TOKEN

docker build \
  --secret id=github_token,env=GITHUB_TOKEN \
  -f mainnet/singularity/Dockerfile \
  --build-arg GIT_REPOSITORY=https://github.com/QuarkChain/pyquarkchain-hot-fix.git \
  --build-arg GIT_TAG=fix/create2-gas \
  -t pyquarkchain:hotfix \
  .

unset GITHUB_TOKEN
```

`GIT_TAG` may be a branch, tag, or commit SHA. Use a full commit SHA for a
reproducible production image.
