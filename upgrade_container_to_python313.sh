#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  echo "Usage: $0 [python-version] [repo-dir]"
  echo "Example: $0 3.13.7 /code/pyquarkchain"
}

if [[ $# -gt 2 ]]; then
  usage
  exit 2
fi

PYTHON_VERSION="${1:-3.13.7}"
REPO_DIR="${2:-/code/pyquarkchain}"
PYTHON_PREFIX="/opt/python-${PYTHON_VERSION}"
VENV_DIR="/opt/venvs/pyquarkchain-py313"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Error: run this script as root inside the container." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "Installing Python ${PYTHON_VERSION} inside the current container..."
echo "Existing Python:"
python3 --version || true

# Debian Buster is archived. Try the existing sources first, then switch to
# archive.debian.org only if apt update fails.
if ! apt-get -o Acquire::Check-Valid-Until=false update; then
  cp -a /etc/apt/sources.list /etc/apt/sources.list.before-python313
  cat >/etc/apt/sources.list <<'EOF'
deb http://archive.debian.org/debian buster main
deb http://archive.debian.org/debian-security buster/updates main
EOF
  apt-get -o Acquire::Check-Valid-Until=false update
fi

apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  wget \
  xz-utils \
  libssl-dev \
  zlib1g-dev \
  libbz2-dev \
  libreadline-dev \
  libsqlite3-dev \
  libffi-dev \
  liblzma-dev \
  libgdbm-dev \
  libncurses5-dev \
  libncursesw5-dev \
  libexpat1-dev \
  uuid-dev \
  tk-dev

BUILD_DIR="/tmp/python-${PYTHON_VERSION}-build"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

wget -O "Python-${PYTHON_VERSION}.tgz" \
  "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
tar -xzf "Python-${PYTHON_VERSION}.tgz"
cd "Python-${PYTHON_VERSION}"

./configure \
  --prefix="$PYTHON_PREFIX" \
  --with-ensurepip=install

make -j"$(nproc)"
make altinstall

"$PYTHON_PREFIX/bin/python3.13" --version
"$PYTHON_PREFIX/bin/python3.13" -m venv "$VENV_DIR"

"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel

if [[ ! -f "$REPO_DIR/requirements.txt" ]]; then
  echo "Error: requirements.txt not found: $REPO_DIR/requirements.txt" >&2
  exit 1
fi

"$VENV_DIR/bin/python" -m pip install -r "$REPO_DIR/requirements.txt"

rm -rf "$BUILD_DIR"
apt-get clean
rm -rf /var/lib/apt/lists/*

echo
echo "Python 3.13 environment installed successfully:"
echo "  Python: $VENV_DIR/bin/python"
echo "  Pip:    $VENV_DIR/bin/pip"
echo "  Repo:   $REPO_DIR"
echo
echo "Run the project with:"
echo "  source $VENV_DIR/bin/activate"
echo "  cd $REPO_DIR"
