#!/bin/bash

function usage {
  echo "usage: $(basename "$0") <location> <version>

location:
  test
    release to test.pypi

  prod
    release to pypi

version:
  a version number
"
}

function version_gt {
  test "$(printf '%s\n' "$@" | sort -V | head -n 1)" != "$1";
}

function check_new_version {
  if [[ "$DISTRIBUTION" == "test" ]]
  then
    distribution_url=https://test.pypi.org/pypi/aitoai/json
  else
    distribution_url=https://pypi.org/pypi/aitoai/json
  fi
  latest_version=$(curl -s "$distribution_url"  | jq .releases | jq 'keys[]' | sort -r | head -n1 | sed 's/\"//g')

  if version_gt "$latest_version" "$NEW_VERSION" || [[ $NEW_VERSION == "$latest_version" ]]
  then
    echo
    echo "error: new version must be greater than the latest version $latest_version"
    echo
    exit 1
  fi
}

function check_git_tree {
  if [[ $(git status --porcelain) ]]
  then
    echo "tree is dirty, please commit changes before release"
    exit 1
  fi
}

function bump_version_setup_file {
  old_version=$(sed -n 's/^VERSION *= \"\(.*\)\"/\1/p' setup.py)
  sed -i -e "s/^VERSION *=.*/VERSION = \"$NEW_VERSION\"/" setup.py
}

function check_version_doc_change_logs {
  if ! grep -q "^#### $NEW_VERSION" docs/change_logs.md ;
  then
    echo
    echo "error: version $NEW_VERSION change log not found in docs/change_logs.md"
    echo
    exit 1
  fi
}

function git_bump_prod_version {
  git commit -am "Bump to $NEW_VERSION"
  git tag "$NEW_VERSION"
  git push
  git push --tags
}

function prepare_release_tools {
  echo "installing pandoc and pypandoc to convert markdown to rst"
  sudo apt-get install pandoc
  pip install --upgrade pypandoc
  export CONVERT_README='true'

  echo "installing twine to release package"
  pip install --upgrade twine
}

function build_package {
  pip install --upgrade setuptools wheel
  python setup.py sdist bdist_wheel
}

if [[ "$1" == "" ]]
then
  echo
  echo "error: specify distribution: test|prod"
  echo
  usage
  exit 1
else
  DISTRIBUTION=$1
fi

if [[ "$2" == "" ]]
then
  echo
  echo "error: missing release version"
  echo
  usage
  exit 1
else
  NEW_VERSION=$2
fi

check_new_version
check_git_tree
bump_version_setup_file
prepare_release_tools
build_package

if [[ "$1" == "test" ]]
then
  twine check dist/* && twine upload --repository-url https://test.pypi.org/legacy/ dist/*
else
  check_version_doc_change_logs
  git_bump_prod_version
  twine check dist/* && twine upload dist/*
fi

exit $!
