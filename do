#!/usr/bin/env bash

COMMAND="$1"

case "$COMMAND" in
  build-dev-docker)
    echo "Building Docker image 'aito-python-tools'..."
    docker build -t aito-python-tools -f Dockerfile.dev .
    ;;

  run-dev-docker)
    echo "Running Docker container from 'aito-python-tools' image..."
    docker run -it --rm \
      -e DISPLAY=$DISPLAY \
      -v /tmp/.X11-unix:/tmp/.X11-unix \
      -e TERM=xterm-256color \
      -v "$(pwd)":/workspace \
      --network host \
      aito-python-tools \
      bash
    ;;

  deploy-docs)
    echo "Building and deploying documentation to GitHub Pages..."
    set -e

    # Check we're at project root
    if [[ ! -f aito/__init__.py ]]; then
      echo "Error: must be run from project root directory"
      exit 1
    fi

    # Activate virtualenv if present
    if [[ -f venv/bin/activate ]]; then
      echo "Activating virtual environment..."
      source venv/bin/activate
    fi

    # Check sphinx-build is available
    if ! command -v sphinx-build &> /dev/null; then
      echo "Error: sphinx-build not found. Install with: pip install -r requirements/docs.txt"
      exit 1
    fi

    # Build docs
    echo "Building Sphinx documentation..."
    cd docs
    make clean html SPHINXOPTS="-W"
    cd ..

    # Get current branch and commit info
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    CURRENT_COMMIT=$(git rev-parse --short HEAD)
    VERSION=$(python -c "import aito; print(aito.__version__)")

    # Clone gh-pages branch to temp directory
    TEMP_DIR=$(mktemp -d)
    echo "Cloning gh-pages branch to $TEMP_DIR..."
    git clone --branch gh-pages --single-branch --depth 1 "$(git remote get-url origin)" "$TEMP_DIR"

    # Clear old content (except .git) and copy new
    echo "Updating documentation..."
    find "$TEMP_DIR" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +
    cp -r docs/build/html/* "$TEMP_DIR/"
    touch "$TEMP_DIR/.nojekyll"

    # Commit and push
    cd "$TEMP_DIR"
    git add -A
    git commit -m "Deploy docs from $CURRENT_BRANCH ($CURRENT_COMMIT) - v$VERSION" || echo "No changes to commit"
    git push origin gh-pages

    # Cleanup
    cd -
    rm -rf "$TEMP_DIR"

    echo "Documentation deployed successfully!"
    echo "View at: https://aitodotai.github.io/aito-python-tools/"
    ;;

  *)
    echo "Usage: $0 {build-dev-docker|run-dev-docker|deploy-docs}"
    exit 1
    ;;
esac
