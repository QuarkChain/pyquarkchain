# Singularity Docker Image

Run the following commands from the repository root.

## Build from the public repository

The default repository is `https://github.com/QuarkChain/pyquarkchain.git`, and
the default revision is `master`:

```bash
docker build \
  --no-cache \
  -f mainnet/singularity/Dockerfile \
  -t "<docker image name>" \
  .
```

## Build from a private repository

Create a GitHub personal access token with read access to the target repository.
For a fine-grained token, grant `Contents: Read-only` permission. If the
organization uses SSO, authorize the token for that organization as well.

Provide the token as a BuildKit secret so it is not stored in an image layer or
passed as a Docker build argument. `GIT_REPOSITORY` must be an HTTPS repository
under the `QuarkChain` organization; the organization and repository URL are
case-insensitive, and the `.git` suffix is optional.

Run the following commands in Bash or zsh:

```bash
printf "GitHub token: "
read -rs GITHUB_TOKEN
echo
export GITHUB_TOKEN

docker build \
  --no-cache \
  --secret id=github_token,env=GITHUB_TOKEN \
  -f mainnet/singularity/Dockerfile \
  --build-arg GIT_REPOSITORY=https://github.com/QuarkChain/pyquarkchain-hot-fix.git \
  --build-arg GIT_TAG="<branch>" \
  -t "<docker image name>" \
  .

unset GITHUB_TOKEN
```

`GIT_TAG` may be a branch, tag, or commit SHA. Use a full commit SHA to pin the
source revision.
