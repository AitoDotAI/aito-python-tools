#!/bin/bash -e

function check_at_top {
  if [[ ! -f aito/__init__.py ]]; then
    echo "this script must be run from the project directory"
    exit 3
  fi
}

function sphinx_build_html {
  cd docs || return
  make clean html SPHINXOPTS="-W"
  cd ..
}

check_at_top
sphinx_build_html
