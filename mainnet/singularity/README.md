# Singularity Docker Image

Run the following commands from the repository root.

## Build from the public repository

The default repository is `https://github.com/QuarkChain/pyquarkchain.git`, and
the default revision is `master`:

```bash
docker build \
  -f mainnet/singularity/Dockerfile \
  -t "<docker image name>" \
  .
```

## Build from a private repository

Create a GitHub personal access token with read access to the target repository.
For a fine-grained token, grant `Contents: Read-only` permission. If the
organization uses SSO, authorize the token for that organization as well.

Provide the token as a BuildKit secret so it is not stored in an image layer or
passed as a Docker build argument:

For Bash:

```bash
read -rsp "GitHub token: " GITHUB_TOKEN
echo
export GITHUB_TOKEN
```

For zsh:

```zsh
read -rs "GITHUB_TOKEN?GitHub token: "
echo
export GITHUB_TOKEN
```

Then build the image:

```sh
docker build \
  --no-cache
  --secret id=github_token,env=GITHUB_TOKEN \
  -f mainnet/singularity/Dockerfile \
  --build-arg GIT_REPOSITORY=https://github.com/QuarkChain/pyquarkchain-hot-fix.git \
  --build-arg GIT_TAG="<branch>" \
  -t "<docker image name>" \
  .

unset GITHUB_TOKEN
```

`GIT_TAG` may be a branch, tag, or commit SHA. Use a full commit SHA for a
reproducible production image.
