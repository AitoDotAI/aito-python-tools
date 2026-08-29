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

    # Activate the virtualenv only if it actually works. A venv whose
    # interpreter has been garbage-collected still has an activate script --
    # a real hazard on Nix, where venv/bin/python3 is a symlink into
    # /nix/store -- so testing for the file alone activates a broken
    # environment and the failure surfaces several steps later as a missing
    # module rather than here.
    if [[ -f venv/bin/activate ]]; then
      if venv/bin/python --version >/dev/null 2>&1; then
        echo "Activating virtual environment..."
        source venv/bin/activate
      else
        echo "Warning: venv/ exists but its interpreter does not run; using the ambient python."
        echo "         Recreate it with: rm -rf venv && python3 -m venv venv"
      fi
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

  release)
    echo "Release aitoai package to PyPI"
    set -e

    # Check we're at project root
    if [[ ! -f aito/__init__.py ]]; then
      echo "Error: must be run from project root directory"
      exit 1
    fi

    # Activate the virtualenv only if it actually works. A venv whose
    # interpreter has been garbage-collected still has an activate script --
    # a real hazard on Nix, where venv/bin/python3 is a symlink into
    # /nix/store -- so testing for the file alone activates a broken
    # environment and the failure surfaces several steps later as a missing
    # module rather than here.
    if [[ -f venv/bin/activate ]]; then
      if venv/bin/python --version >/dev/null 2>&1; then
        echo "Activating virtual environment..."
        source venv/bin/activate
      else
        echo "Warning: venv/ exists but its interpreter does not run; using the ambient python."
        echo "         Recreate it with: rm -rf venv && python3 -m venv venv"
      fi
    fi

    # Check required tools
    for cmd in twine python3; do
      if ! command -v $cmd &> /dev/null; then
        echo "Error: $cmd not found. Install with: pip install -r requirements/deploy.txt"
        exit 1
      fi
    done

    # Get version info
    VERSION=$(python3 -c "import aito; print(aito.__version__)")
    echo "Current version in code: $VERSION"

    # Check PyPI for existing versions
    LATEST_PYPI=$(python3 -c "import requests; versions=list(requests.get('https://pypi.org/pypi/aitoai/json').json()['releases'].keys()); print(sorted(versions)[-1])")
    echo "Latest version on PyPI: $LATEST_PYPI"

    if [[ "$VERSION" == "$LATEST_PYPI" ]]; then
      echo "Error: Version $VERSION already exists on PyPI"
      exit 1
    fi

    # Check git status
    if [[ -n $(git status --porcelain) ]]; then
      echo "Error: Working tree is dirty. Commit changes before release."
      exit 1
    fi

    # Check we're on master
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$BRANCH" != "master" ]]; then
      echo "Warning: Not on master branch (currently on $BRANCH)"
      read -p "Continue anyway? [y/N] " -n 1 -r
      echo
      if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
      fi
    fi

    # Check changelog exists for this version
    if ! grep -q "^$VERSION" docs/source/changelog.rst; then
      echo "Error: Changelog entry for $VERSION not found in docs/source/changelog.rst"
      exit 1
    fi

    echo ""
    echo "=== Release Checklist ==="
    echo "  Version: $VERSION"
    echo "  Branch: $BRANCH"
    echo "  Changelog: OK"
    echo ""

    read -p "Proceed with release to PyPI? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "Aborted."
      exit 1
    fi

    # Build package
    echo "Building package..."
    rm -rf dist/
    python3 setup.py sdist bdist_wheel
    twine check dist/*

    # Deploy docs BEFORE publishing. deploy-docs builds with -W, so any doc
    # problem is a hard failure, and this step used to run *after* the upload:
    # a failure there left a version live on PyPI with no matching docs and no
    # git tag, and PyPI will not accept that version number a second time.
    # Docs are idempotent and re-deployable; the upload is permanent, so the
    # irreversible step goes last.
    echo "Deploying documentation..."
    ./do deploy-docs

    # Upload to PyPI
    echo "Uploading to PyPI..."
    twine upload dist/*

    # Tag release
    echo "Creating git tag..."
    git tag "$VERSION"
    git push origin "$VERSION"

    echo ""
    echo "=== Release Complete ==="
    echo "  PyPI: https://pypi.org/project/aitoai/$VERSION/"
    echo "  Docs: https://aitodotai.github.io/aito-python-tools/"
    echo "  Tag: $VERSION"
    ;;

  *)
    echo "Usage: $0 {build-dev-docker|run-dev-docker|deploy-docs|release}"
    exit 1
    ;;
esac
